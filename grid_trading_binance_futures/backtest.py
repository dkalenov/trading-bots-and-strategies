"""
Grid Trading Bot — Backtester.

Simulates grid trading on historical Binance Futures data.
Places N buy orders below price and N sell orders above.
Tracks complete grid cycles (buy → sell) for realistic win rate.
"""

import os
import csv
import io
import zipfile
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pytz import timezone


def download_klines(symbol, interval, start_date, end_date, klines_dir='klines'):
    start = datetime.strptime(start_date, '%Y-%m')
    end = datetime.strptime(end_date, '%Y-%m')
    months = []
    current = start
    while current <= end:
        months.append(current.strftime('%Y-%m'))
        current += relativedelta(months=1)

    os.makedirs(klines_dir, exist_ok=True)
    klines = {'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': []}

    for month in months:
        filename = f"{symbol}-{interval}-{month}.zip"
        file_path = os.path.join(klines_dir, filename)

        if not os.path.exists(file_path) or os.path.getsize(file_path) < 100:
            url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{interval}/{filename}"
            try:
                r = requests.get(url, allow_redirects=True, timeout=30)
                if r.status_code == 200 and len(r.content) > 100:
                    with open(file_path, 'wb') as f:
                        f.write(r.content)
                else:
                    continue
            except Exception:
                continue

        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                csv_name = f"{symbol}-{interval}-{month}.csv"
                with zf.open(csv_name, 'r') as csv_file:
                    reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
                    for row in reader:
                        if row[0].isdigit():
                            ts = int(row[0])
                            if ts > 1e15:
                                ts = ts / 1000
                            klines['Date'].append(
                                datetime.fromtimestamp(ts / 1000, tz=timezone('UTC'))
                            )
                            klines['Open'].append(float(row[1]))
                            klines['High'].append(float(row[2]))
                            klines['Low'].append(float(row[3]))
                            klines['Close'].append(float(row[4]))
                            klines['Volume'].append(float(row[5]))
        except (zipfile.BadZipFile, KeyError):
            continue

    if not klines['Date']:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.DataFrame(klines)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    return df


class GridBacktester:
    """
    Grid trading backtester.

    Logic:
    1. Place N buy orders below current price, N sell orders above
    2. When a buy fills → add to open_entries (FIFO queue)
    3. When you have entries → sell at TP level (oldest entry first)
    4. When a sell fills → close oldest entry, realize PnL
    5. After sell → place new buy at lower grid level
    6. Track: complete cycles, open positions, drawdown
    """

    def __init__(self, initial_capital=10000, n_levels=10, proportion=0.03,
                 volume=0.05, tp_pct=5.0, commission_rate=0.0004, slippage_rate=0.0005):
        self.initial_capital = initial_capital
        self.n_levels = n_levels
        self.proportion = proportion
        self.volume = volume
        self.tp_pct = tp_pct
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

    def run(self, df):
        capital = self.initial_capital
        trades = []
        open_entries = []  # FIFO queue of entry prices
        pending_buys = []  # grid buy orders waiting to fill
        pending_sells = []  # grid sell orders waiting to fill
        equity_curve = [capital]
        grid_center = None
        grid_redraw_threshold = self.proportion  # redraw when price moves this %

        for i in range(len(df)):
            low = df['Low'].iloc[i]
            high = df['High'].iloc[i]
            close = df['Close'].iloc[i]
            timestamp = df.index[i]

            # Initialize grid at start
            if grid_center is None:
                grid_center = close
                pending_buys = self._generate_buys(grid_center)
                pending_sells = self._generate_sells(grid_center)

            # Redraw grid if price moved far from center
            if grid_center is not None and len(pending_buys) == 0 and len(pending_sells) == 0:
                grid_center = close
                pending_buys = self._generate_buys(grid_center)
                pending_sells = self._generate_sells(grid_center)

            # Also redraw if price moved > threshold from grid center
            if grid_center is not None:
                pct_from_center = abs(close - grid_center) / grid_center * 100
                if pct_from_center > grid_redraw_threshold * self.n_levels * 0.5:
                    # Price escaped the grid — redraw around current price
                    # But keep open entries
                    grid_center = close
                    pending_buys = self._generate_buys(grid_center)
                    pending_sells = self._generate_sells(grid_center)

            # ── Check BUY fills (price drops to buy level) ──
            newly_filled = []
            for buy_price in pending_buys[:]:
                if low <= buy_price:
                    fill_price = buy_price * (1 + self.slippage_rate)
                    cost = fill_price * self.volume
                    commission = cost * self.commission_rate
                    if capital >= cost + commission:
                        capital -= (cost + commission)
                        open_entries.append(fill_price)
                        trades.append({
                            'time': timestamp, 'side': 'BUY', 'price': fill_price,
                            'qty': self.volume, 'pnl': 0, 'commission': commission
                        })
                        newly_filled.append(buy_price)
            for p in newly_filled:
                pending_buys.remove(p)

            # ── Check SELL fills (price rises to TP level) ──
            newly_sold = []
            for sell_price in pending_sells[:]:
                if high >= sell_price and open_entries:
                    entry_price = open_entries.pop(0)  # FIFO
                    fill_price = sell_price * (1 - self.slippage_rate)
                    pnl = (fill_price - entry_price) * self.volume
                    commission = fill_price * self.volume * self.commission_rate
                    net_pnl = pnl - commission
                    capital += fill_price * self.volume - commission
                    trades.append({
                        'time': timestamp, 'side': 'SELL', 'price': fill_price,
                        'qty': self.volume, 'pnl': net_pnl, 'commission': commission,
                        'entry_price': entry_price
                    })
                    newly_sold.append(sell_price)
            for p in newly_sold:
                pending_sells.remove(p)

            # ── Track equity ──
            unrealized = sum((close - e) * self.volume for e in open_entries)
            equity = capital + sum(e * self.volume for e in open_entries) + unrealized
            equity_curve.append(equity)

        # ── Close remaining position at last price ──
        last_price = df['Close'].iloc[-1]
        if open_entries:
            for entry_price in open_entries:
                pnl = (last_price - entry_price) * self.volume
                commission = last_price * self.volume * self.commission_rate
                net_pnl = pnl - commission
                capital += last_price * self.volume - commission
                trades.append({
                    'time': df.index[-1], 'side': 'FORCE_CLOSE', 'price': last_price,
                    'qty': self.volume, 'pnl': net_pnl, 'commission': commission,
                    'entry_price': entry_price
                })

        return self._compute_stats(trades, equity_curve)

    def _generate_buys(self, center_price):
        """Generate N buy limit orders below current price."""
        orders = []
        for i in range(1, self.n_levels + 1):
            pct = i * self.proportion
            price = round(center_price * (1 - pct / 100), 2)
            orders.append(price)
        return orders

    def _generate_sells(self, center_price):
        """Generate N sell limit orders above current price."""
        orders = []
        for i in range(1, self.n_levels + 1):
            pct = i * self.proportion
            price = round(center_price * (1 + pct / 100), 2)
            orders.append(price)
        return orders

    def _compute_stats(self, trades, equity_curve):
        sell_trades = [t for t in trades if t['side'] in ('SELL', 'FORCE_CLOSE')]
        buy_trades = [t for t in trades if t['side'] == 'BUY']

        total_return = (equity_curve[-1] - self.initial_capital) / self.initial_capital * 100

        # Max drawdown
        peak = equity_curve[0]
        max_dd = 0.0
        for eq in equity_curve:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        # Win rate: profitable exits / total exits
        wins = [t for t in sell_trades if t['pnl'] > 0]
        losses = [t for t in sell_trades if t['pnl'] <= 0]
        win_rate = (len(wins) / len(sell_trades) * 100) if sell_trades else 0

        # PnL per exit
        exit_pnls = [t['pnl'] for t in sell_trades]
        avg_pnl = np.mean(exit_pnls) if exit_pnls else 0
        total_pnl = sum(exit_pnls)
        total_commission = sum(t['commission'] for t in trades)

        # Profit factor
        gross_profit = sum(t['pnl'] for t in wins)
        gross_loss = abs(sum(t['pnl'] for t in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        # Forced closes = positions stuck in loss
        forced = [t for t in trades if t['side'] == 'FORCE_CLOSE']

        return {
            'initial_capital': self.initial_capital,
            'final_capital': round(equity_curve[-1], 2),
            'total_return_pct': round(total_return, 2),
            'total_trades': len(trades),
            'buy_fills': len(buy_trades),
            'sell_fills': len([t for t in trades if t['side'] == 'SELL']),
            'forced_closes': len(forced),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': round(win_rate, 1),
            'avg_pnl_per_exit': round(avg_pnl, 2),
            'total_pnl': round(total_pnl, 2),
            'total_commission': round(total_commission, 2),
            'profit_factor': round(profit_factor, 2),
            'max_drawdown_pct': round(max_dd, 2),
            'trades': trades,
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Grid Trading Backtester')
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--interval', default='1h')
    parser.add_argument('--start', default='2024-01')
    parser.add_argument('--end', default='2025-06')
    parser.add_argument('--n-levels', type=int, default=10)
    parser.add_argument('--proportion', type=float, default=3.0)
    parser.add_argument('--volume', type=float, default=0.05)
    parser.add_argument('--tp', type=float, default=5.0)
    parser.add_argument('--capital', type=float, default=10000)
    parser.add_argument('--slippage', type=float, default=0.05, help='Slippage in %%')
    args = parser.parse_args()

    print(f"\nGrid Trading Backtester — {args.symbol} {args.interval}")
    print(f"{'='*60}")
    print(f"Period:    {args.start} — {args.end}")
    print(f"Grid:      {args.n_levels} levels, {args.proportion}% spacing")
    print(f"Volume:    {args.volume} per level")
    print(f"TP:        {args.tp}%")
    print(f"Slippage:  {args.slippage}%")
    print(f"Capital:   ${args.capital:,.0f}")
    print()

    df = download_klines(args.symbol, args.interval, args.start, args.end)
    if len(df) == 0:
        print("No data loaded. Check symbol/interval/dates.")
        return
    print(f"Loaded {len(df)} candles: {df.index[0]} — {df.index[-1]}")

    bt = GridBacktester(
        initial_capital=args.capital,
        n_levels=args.n_levels,
        proportion=args.proportion,
        volume=args.volume,
        tp_pct=args.tp,
        slippage_rate=args.slippage / 100,
    )
    stats = bt.run(df)

    print(f"\n{'='*60}")
    print(f"  GRID TRADING RESULTS — {args.symbol}")
    print(f"{'='*60}")
    print(f"  Initial Capital:    ${stats['initial_capital']:,.2f}")
    print(f"  Final Capital:      ${stats['final_capital']:,.2f}")
    print(f"  Total Return:       {stats['total_return_pct']:+.2f}%")
    print(f"  Max Drawdown:       {stats['max_drawdown_pct']:.2f}%")
    print(f"  ─────────────────────────────────────────")
    print(f"  Buy Fills:          {stats['buy_fills']}")
    print(f"  Sell Fills:         {stats['sell_fills']}")
    print(f"  Forced Closes:      {stats['forced_closes']}")
    print(f"  Wins:               {stats['wins']}")
    print(f"  Losses:             {stats['losses']}")
    print(f"  Win Rate:           {stats['win_rate']:.1f}%")
    print(f"  Profit Factor:      {stats['profit_factor']:.2f}")
    print(f"  Avg PnL/exit:       ${stats['avg_pnl_per_exit']:.2f}")
    print(f"  Total PnL:          ${stats['total_pnl']:.2f}")
    print(f"  Total Commission:   ${stats['total_commission']:.2f}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
