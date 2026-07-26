"""
Candlestick Pattern Strategy — Backtester.

Downloads data from Bybit, runs pattern detection with filters,
and simulates trading with ATR-based SL/TP.
"""

import os
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from collections import Counter

from strategy import detect_patterns, apply_filters


# ── Data download ──────────────────────────────────────────────────────

BYBIT_INTERVAL_MAP = {
    '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
    '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
    '1d': 'D', '1w': 'W', '1M': 'M',
}

INTERVAL_MS = {
    '1m': 60_000, '5m': 300_000, '15m': 900_000, '30m': 1_800_000,
    '1h': 3_600_000, '4h': 14_400_000, '1d': 86_400_000,
}


def download_klines(symbol, interval, start_date, end_date, klines_dir='klines'):
    os.makedirs(klines_dir, exist_ok=True)
    cache_file = os.path.join(klines_dir, f"{symbol}-{interval}-{start_date}-{end_date}.csv")

    if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
        print(f"Loading cached: {cache_file}")
        df = pd.read_csv(cache_file, parse_dates=['Date'])
        df.set_index('Date', inplace=True)
        return df

    interval_ms = INTERVAL_MS.get(interval)
    if interval_ms is None:
        raise ValueError(f"Unknown interval: {interval}")

    start_ts = int(datetime.strptime(start_date, '%Y-%m').replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_dt = datetime.strptime(end_date, '%Y-%m')
    if end_dt.month == 12:
        end_dt = end_dt.replace(year=end_dt.year + 1, month=1)
    else:
        end_dt = end_dt.replace(month=end_dt.month + 1)
    end_ts = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    all_klines = []
    current_start = start_ts

    while current_start < end_ts:
        bybit_interval = BYBIT_INTERVAL_MAP.get(interval, interval)
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': bybit_interval,
            'start': str(current_start),
            'limit': '200',
        }

        try:
            resp = requests.get('https://api.bybit.com/v5/market/kline',
                                params=params, timeout=30)
            data = resp.json()
            if data.get('retCode') != 0:
                print(f"API error: {data.get('retMsg')}")
                break

            klines = data.get('result', {}).get('list', [])
            if not klines:
                break

            for k in klines:
                ts = int(k[0])
                if ts > end_ts:
                    break
                all_klines.append({
                    'Date': datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    'Open': float(k[1]),
                    'High': float(k[2]),
                    'Low': float(k[3]),
                    'Close': float(k[4]),
                    'Volume': float(k[5]),
                })

            newest_ts = int(klines[0][0])
            current_start = newest_ts + interval_ms
            time.sleep(0.1)

        except Exception as e:
            print(f"Error: {e}")
            break

    if not all_klines:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.DataFrame(all_klines)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    df.to_csv(cache_file)
    print(f"Saved {len(df)} klines to {cache_file}")
    return df


# ── Backtest engine ────────────────────────────────────────────────────

class Backtester:
    def __init__(self, initial_capital=10000, risk_pct=0.01,
                 leverage=20, commission=0.0004, slippage=0.0002):
        self.initial_capital = initial_capital
        self.risk_pct = risk_pct
        self.leverage = leverage
        self.commission = commission
        self.slippage = slippage

    def run(self, df, sl_atr=2.0, tp_atr=4.0, min_strength=1.3,
            atr_period=14, min_body_atr=0.15,
            use_trend_filter=True, ema_fast=50, ema_slow=200,
            min_atr_pct=0.3, patterns_only=None):
        """
        Run backtest with pattern signals and optional filters.

        Args:
            df: OHLCV DataFrame
            sl_atr: Stop loss = N x ATR
            tp_atr: Take profit = N x ATR
            min_strength: Min signal strength
            atr_period: ATR calculation period
            min_body_atr: Min candle body size
            use_trend_filter: Enable EMA trend filter
            ema_fast: Fast EMA period
            ema_slow: Slow EMA period
            min_atr_pct: Min ATR as % of price
            patterns_only: List of pattern names to keep (None = all)
        """
        # Detect patterns
        df = detect_patterns(df, atr_period=atr_period, min_body_atr=min_body_atr)

        # Apply filters
        if use_trend_filter or min_strength > 0 or min_atr_pct > 0 or patterns_only:
            df = apply_filters(df, ema_fast=ema_fast, ema_slow=ema_slow,
                               min_strength=min_strength, min_atr_pct=min_atr_pct,
                               patterns_only=patterns_only)

        df = df.dropna(subset=['ATR'])

        capital = self.initial_capital
        peak = capital
        max_dd = 0
        position = None
        trades = []
        equity = [capital]

        warmup = max(ema_slow + 5 if use_trend_filter else atr_period + 2, 5)

        for i in range(warmup, len(df)):
            close = float(df['Close'].iloc[i])
            high = float(df['High'].iloc[i])
            low = float(df['Low'].iloc[i])
            signal = int(df['Signal'].iloc[i])
            strength = float(df['Signal_strength'].iloc[i])
            atr = float(df['ATR'].iloc[i])
            timestamp = df.index[i]

            # Check exit
            if position is not None:
                exit_price = None
                exit_reason = ''

                if position['side'] == 'LONG':
                    if low <= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif high >= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == -1:
                        exit_price, exit_reason = close, 'SIGNAL_SELL'
                else:
                    if high >= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif low <= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == 1:
                        exit_price, exit_reason = close, 'SIGNAL_BUY'

                if exit_price is not None:
                    notional = position['notional']
                    if position['side'] == 'LONG':
                        gross_pnl = (exit_price - position['entry_price']) * position['quantity']
                    else:
                        gross_pnl = (position['entry_price'] - exit_price) * position['quantity']

                    exit_commission = notional * self.commission
                    exit_slippage = notional * self.slippage
                    net_pnl = gross_pnl - position['entry_commission'] - exit_commission - exit_slippage

                    trades.append({
                        'entry_time': position['entry_time'],
                        'exit_time': timestamp,
                        'side': position['side'],
                        'entry_price': position['entry_price'],
                        'exit_price': exit_price,
                        'pnl': net_pnl,
                        'exit_reason': exit_reason,
                        'pattern': position['pattern'],
                        'bars_held': i - position['entry_bar'],
                    })
                    capital += net_pnl
                    position = None

            # Check entry
            if position is None and signal != 0 and capital > 100:
                if signal == 1:
                    entry_price = close * (1 + self.slippage)
                    sl = entry_price - sl_atr * atr
                    tp = entry_price + tp_atr * atr
                    side = 'LONG'
                else:
                    entry_price = close * (1 - self.slippage)
                    sl = entry_price + sl_atr * atr
                    tp = entry_price - tp_atr * atr
                    side = 'SHORT'

                sl_distance = abs(entry_price - sl)
                if sl_distance <= 0:
                    equity.append(capital)
                    continue

                max_loss = capital * self.risk_pct
                quantity = max_loss / sl_distance
                notional = quantity * entry_price
                max_notional = capital * self.leverage
                if notional > max_notional:
                    quantity = max_notional / entry_price
                    notional = max_notional

                if notional < 5:
                    equity.append(capital)
                    continue

                entry_commission = notional * self.commission

                position = {
                    'side': side,
                    'entry_price': entry_price,
                    'entry_time': timestamp,
                    'entry_bar': i,
                    'stop_loss': sl,
                    'take_profit': tp,
                    'quantity': quantity,
                    'notional': notional,
                    'entry_commission': entry_commission,
                    'pattern': str(df['Pattern'].iloc[i]),
                }

            # Track equity
            unrealized = 0
            if position is not None:
                if position['side'] == 'LONG':
                    unrealized = (close - position['entry_price']) * position['quantity']
                else:
                    unrealized = (position['entry_price'] - close) * position['quantity']
            equity.append(capital + unrealized)

            # Track drawdown
            current_eq = capital + unrealized
            if current_eq > peak:
                peak = current_eq
            dd = (peak - current_eq) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        # Close open position at end
        if position is not None:
            exit_price = float(df['Close'].iloc[-1])
            if position['side'] == 'LONG':
                gross_pnl = (exit_price - position['entry_price']) * position['quantity']
            else:
                gross_pnl = (position['entry_price'] - exit_price) * position['quantity']
            net_pnl = gross_pnl - position['entry_commission'] - position['notional'] * self.commission - position['notional'] * self.slippage
            trades.append({
                'entry_time': position['entry_time'],
                'exit_time': df.index[-1],
                'side': position['side'],
                'entry_price': position['entry_price'],
                'exit_price': exit_price,
                'pnl': net_pnl,
                'exit_reason': 'END_OF_DATA',
                'pattern': position['pattern'],
                'bars_held': len(df) - 1 - position['entry_bar'],
            })
            capital += net_pnl

        return self._compute_stats(trades, equity, max_dd), trades

    def _compute_stats(self, trades, equity, max_dd):
        stats = {}
        stats['initial_capital'] = self.initial_capital
        stats['final_capital'] = equity[-1] if equity else self.initial_capital
        stats['total_return_pct'] = (stats['final_capital'] - self.initial_capital) / self.initial_capital * 100
        stats['total_trades'] = len(trades)
        stats['max_drawdown_pct'] = max_dd

        if not trades:
            stats['win_rate'] = 0
            stats['profit_factor'] = 0
            stats['sharpe_ratio'] = 0
            return stats

        winners = [t for t in trades if t['pnl'] > 0]
        losers = [t for t in trades if t['pnl'] <= 0]
        stats['win_rate'] = len(winners) / len(trades) * 100
        stats['winning_trades'] = len(winners)
        stats['losing_trades'] = len(losers)

        total_win = sum(t['pnl'] for t in winners)
        total_loss = abs(sum(t['pnl'] for t in losers))
        stats['profit_factor'] = total_win / total_loss if total_loss > 0 else float('inf')

        # Sharpe
        returns = np.diff(equity) / np.array(equity[:-1])
        returns = [r for r in returns if np.isfinite(r)]
        if len(returns) > 1 and np.std(returns) > 0:
            stats['sharpe_ratio'] = np.mean(returns) / np.std(returns) * np.sqrt(8760)
        else:
            stats['sharpe_ratio'] = 0

        # Pattern breakdown
        pattern_counts = Counter(t['pattern'] for t in trades)
        stats['pattern_breakdown'] = dict(pattern_counts.most_common())

        # Exit reason breakdown
        reason_counts = Counter(t['exit_reason'] for t in trades)
        stats['exit_reasons'] = dict(reason_counts.most_common())

        return stats


# ── CLI ────────────────────────────────────────────────────────────────

def print_stats(stats):
    print(f"\n{'='*60}")
    print(f"  Candle Pattern Backtest")
    print(f"{'='*60}")
    print(f"  Initial Capital:  ${stats['initial_capital']:,.2f}")
    print(f"  Final Capital:    ${stats['final_capital']:,.2f}")
    print(f"  Total Return:     {stats['total_return_pct']:+.2f}%")
    print(f"  Max Drawdown:     {stats['max_drawdown_pct']:.2f}%")
    print(f"  Sharpe Ratio:     {stats['sharpe_ratio']:.3f}")
    print(f"  Win Rate:         {stats['win_rate']:.1f}%")
    print(f"  Total Trades:     {stats['total_trades']}")
    print(f"  Winning:          {stats.get('winning_trades', 0)}")
    print(f"  Losing:           {stats.get('losing_trades', 0)}")
    print(f"  Profit Factor:    {stats['profit_factor']:.2f}")
    if 'pattern_breakdown' in stats:
        print(f"\n  Pattern Breakdown:")
        for pat, count in stats['pattern_breakdown'].items():
            print(f"    {pat}: {count}")
    if 'exit_reasons' in stats:
        print(f"\n  Exit Reasons:")
        for reason, count in stats['exit_reasons'].items():
            print(f"    {reason}: {count}")
    print(f"{'='*60}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Candle Pattern Backtest')
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--interval', default='1h')
    parser.add_argument('--start', default='2024-01')
    parser.add_argument('--end', default='2025-06')
    parser.add_argument('--sl-atr', type=float, default=2.0, help='SL = N x ATR')
    parser.add_argument('--tp-atr', type=float, default=4.0, help='TP = N x ATR')
    parser.add_argument('--min-strength', type=float, default=1.3)
    parser.add_argument('--capital', type=float, default=10000)
    parser.add_argument('--no-trend-filter', action='store_true', help='Disable EMA trend filter')
    parser.add_argument('--patterns-only', nargs='+', default=None,
                        help='Only trade these patterns (e.g. three_white_soldiers morning_star)')
    parser.add_argument('--baseline', action='store_true', help='Run without filters (baseline)')

    args = parser.parse_args()

    print(f"Downloading {args.symbol} {args.interval} ({args.start} to {args.end})...")
    df = download_klines(args.symbol, args.interval, args.start, args.end)
    print(f"Data: {len(df)} candles")

    if args.baseline:
        # Baseline: no filters
        bt = Backtester(initial_capital=args.capital, risk_pct=0.01, leverage=20)
        stats, trades = bt.run(df, sl_atr=0.75, tp_atr=0.75, min_strength=0.0,
                               use_trend_filter=False, min_atr_pct=0)
        print_stats(stats)
    else:
        # Default: optimized config
        bt = Backtester(initial_capital=args.capital, risk_pct=0.01, leverage=20)
        stats, trades = bt.run(df, sl_atr=args.sl_atr, tp_atr=args.tp_atr,
                               min_strength=args.min_strength,
                               use_trend_filter=not args.no_trend_filter,
                               patterns_only=args.patterns_only)
        print_stats(stats)
