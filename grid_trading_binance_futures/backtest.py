"""
Grid Trading Bot — Backtester.

Simulates the SAME strategy that binance_bot.py trades live:
- N buy levels below price, N sell levels above price (see grid_strategy.py)
- First side to fill sets the direction; the opposite side is cancelled
- Same-direction grid orders stay live so the position can average in
- One dynamic take-profit order for the whole averaged position
  (tp_pct = % ROI on margin used, so it scales with leverage)
- Optional stop-loss (stop_loss_pct) and a simplified liquidation check
  (applies even at leverage=1 — a 1x SHORT can be liquidated if price
  roughly doubles against it; see grid_strategy.estimate_liquidation_price)

Modeling choices / known simplifications (see README "Limitations"):
- Intra-candle fill order is inferred from candle direction (close>=open
  => assume open->low->high->close, else open->high->low->close). This
  reduces, but does not eliminate, OHLC look-ahead bias.
- Liquidation price ignores maintenance-margin tiers and funding.
- Fills assume full size executes at the level price plus slippage —
  no partial fills / order-book depth simulation.
- Only ONE grid "cycle" is open at a time (matches the live bot: it does
  not run independent concurrent long+short grids).
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

from grid_strategy import (
    GridConfig,
    generate_grid_levels,
    calculate_tp_price,
    calculate_stop_price,
    estimate_liquidation_price,
    weighted_average_entry,
)


def download_klines(symbol, interval, start_date, end_date, klines_dir='klines'):
    """Download+parse monthly kline zips from Binance Vision (USD-M futures),
    using local cache in `klines_dir` if already present. Warns on any gap
    in the resulting series."""
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
                    print(f"  [warn] no data for {month} (HTTP {r.status_code}) — skipping")
                    continue
            except Exception as e:
                print(f"  [warn] failed to download {month}: {e} — skipping")
                continue

        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                csv_name = f"{symbol}-{interval}-{month}.csv"
                with zf.open(csv_name, 'r') as csv_file:
                    reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
                    for row in reader:
                        if not row or not row[0].isdigit():
                            continue  # skip header row(s) / blank lines
                        ts = int(row[0])
                        if ts > 1e15:       # microsecond timestamps (newer Binance format)
                            ts = ts / 1000  # -> milliseconds
                        klines['Date'].append(
                            datetime.fromtimestamp(ts / 1000, tz=timezone('UTC'))
                        )
                        klines['Open'].append(float(row[1]))
                        klines['High'].append(float(row[2]))
                        klines['Low'].append(float(row[3]))
                        klines['Close'].append(float(row[4]))
                        klines['Volume'].append(float(row[5]))
        except (zipfile.BadZipFile, KeyError) as e:
            print(f"  [warn] corrupt archive for {month}: {e} — skipping")
            continue

    if not klines['Date']:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.DataFrame(klines)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.drop_duplicates(subset='Date').sort_values('Date')
    df.set_index('Date', inplace=True)

    if len(df) > 1:
        deltas = df.index.to_series().diff().dropna()
        expected = deltas.mode()[0] if len(deltas) else None
        gaps = deltas[deltas != expected]
        if len(gaps) > 0:
            print(f"  [warn] {len(gaps)} gap(s) detected in candle series (expected spacing: {expected})")

    return df


class GridBacktester:
    """
    Grid trading backtester — mirrors the live bot's state machine:
    FLAT -> (LONG or SHORT, averaging) -> TP / STOP / LIQUIDATION / end-of-data -> FLAT
    """

    def __init__(self, cfg: GridConfig, initial_capital=10000.0,
                 commission_rate=0.0004, slippage_rate=0.0005):
        self.cfg = cfg
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self._reset_state()

    def _reset_state(self):
        self._balance = self.initial_capital
        self._used_margin = 0.0
        self._direction = 'FLAT'          # 'FLAT' | 'LONG' | 'SHORT'
        self._position_amt = 0.0          # unsigned size
        self._avg_entry = 0.0
        self._tp_price = None
        self._pending_buys = []
        self._pending_sells = []
        self._fills = []
        self._cycles = []
        self._cur_cycle_fills = []

    def _close_position(self, price, reason, ts):
        """Close the whole open position at `price`, realize PnL/commission,
        record the fill + completed cycle, reset to FLAT, and clear any
        resting grid orders so a fresh grid gets drawn on the next bar."""
        qty = self._position_amt
        notional = price * qty
        commission = notional * self.commission_rate
        if self._direction == 'LONG':
            pnl = (price - self._avg_entry) * qty - commission
        else:
            pnl = (self._avg_entry - price) * qty - commission
        self._balance += pnl

        side = {'TP': 'TP_' + ('SELL' if self._direction == 'LONG' else 'BUY'),
                'STOP_LOSS': 'SL_' + ('SELL' if self._direction == 'LONG' else 'BUY'),
                'LIQUIDATION': 'LIQUIDATION',
                'END_OF_DATA': 'END_OF_DATA_CLOSE'}[reason]
        f = {'time': ts, 'side': side, 'price': price, 'qty': qty, 'pnl': pnl, 'commission': commission}
        self._fills.append(f)
        self._cur_cycle_fills.append(f)
        self._cycles.append(self._close_cycle(self._cur_cycle_fills, reason))
        self._cur_cycle_fills = []
        self._direction, self._position_amt, self._avg_entry, self._tp_price = 'FLAT', 0.0, 0.0, None
        self._used_margin = 0.0
        self._pending_buys, self._pending_sells = [], []

    def _try_fill_entries(self, pending, price_ok_fn, side_label, ts):
        """Fill any resting grid entry orders in `pending` (a list belonging
        to self._pending_buys / self._pending_sells) whose level satisfies
        price_ok_fn(level), respecting free margin. Mutates in place."""
        cfg = self.cfg
        filled_levels = [p for p in pending if price_ok_fn(p)]
        for level in filled_levels:
            fill_price = level * (1 + self.slippage_rate) if side_label == 'BUY' else level * (1 - self.slippage_rate)
            notional = fill_price * cfg.volume
            margin_req = notional / cfg.leverage
            commission = notional * self.commission_rate
            if self._balance - self._used_margin < margin_req + commission:
                continue  # not enough free margin — order stays pending
            self._balance -= commission
            self._avg_entry = weighted_average_entry(self._avg_entry, self._position_amt, fill_price, cfg.volume)
            self._position_amt += cfg.volume
            self._used_margin = self._avg_entry * self._position_amt / cfg.leverage
            self._direction = 'LONG' if side_label == 'BUY' else 'SHORT'
            signed_amt = self._position_amt if side_label == 'BUY' else -self._position_amt
            self._tp_price = calculate_tp_price(self._avg_entry, signed_amt, cfg)
            f = {'time': ts, 'side': side_label, 'price': fill_price, 'qty': cfg.volume,
                 'pnl': 0.0, 'commission': commission}
            self._fills.append(f)
            self._cur_cycle_fills.append(f)
            pending.remove(level)
            # opposite-side grid entries are cancelled the moment a direction is established
            if side_label == 'BUY':
                self._pending_sells = []
            else:
                self._pending_buys = []

    def run(self, df: pd.DataFrame) -> dict:
        cfg = self.cfg
        self._reset_state()
        equity_curve = []

        for i in range(len(df)):
            o = df['Open'].iloc[i]
            h = df['High'].iloc[i]
            l = df['Low'].iloc[i]
            c = df['Close'].iloc[i]
            ts = df.index[i]

            # Redraw grid if flat with nothing resting — center on this bar's
            # OPEN (the only price causally known before this bar's H/L happen)
            if self._direction == 'FLAT' and not self._pending_buys and not self._pending_sells:
                self._pending_buys, self._pending_sells = generate_grid_levels(o, cfg)

            signed_amt = self._position_amt if self._direction == 'LONG' else -self._position_amt
            liq_price = estimate_liquidation_price(self._avg_entry, signed_amt, cfg.leverage) \
                if self._direction != 'FLAT' else None
            sl_price = calculate_stop_price(self._avg_entry, signed_amt, cfg) \
                if self._direction != 'FLAT' else None

            bullish = c >= o
            event_order = ['low', 'high'] if bullish else ['high', 'low']

            for ev in event_order:
                if ev == 'low':
                    if self._direction == 'LONG':
                        if sl_price is not None and l <= sl_price:
                            self._close_position(sl_price, 'STOP_LOSS', ts)
                        elif liq_price is not None and l <= liq_price:
                            self._close_position(liq_price, 'LIQUIDATION', ts)
                    if self._direction in ('FLAT', 'LONG'):
                        self._try_fill_entries(self._pending_buys, lambda p: l <= p, 'BUY', ts)
                    elif self._direction == 'SHORT':
                        if self._tp_price is not None and l <= self._tp_price:
                            self._close_position(self._tp_price * (1 + self.slippage_rate), 'TP', ts)

                else:  # 'high'
                    if self._direction == 'SHORT':
                        if sl_price is not None and h >= sl_price:
                            self._close_position(sl_price, 'STOP_LOSS', ts)
                        elif liq_price is not None and h >= liq_price:
                            self._close_position(liq_price, 'LIQUIDATION', ts)
                    if self._direction in ('FLAT', 'SHORT'):
                        self._try_fill_entries(self._pending_sells, lambda p: h >= p, 'SELL', ts)
                    elif self._direction == 'LONG':
                        if self._tp_price is not None and h >= self._tp_price:
                            self._close_position(self._tp_price * (1 - self.slippage_rate), 'TP', ts)

            if self._direction == 'LONG':
                unrealized = (c - self._avg_entry) * self._position_amt
            elif self._direction == 'SHORT':
                unrealized = (self._avg_entry - c) * self._position_amt
            else:
                unrealized = 0.0
            equity_curve.append((ts, self._balance + unrealized))

        if self._direction != 'FLAT':
            self._close_position(df['Close'].iloc[-1], 'END_OF_DATA', df.index[-1])
            equity_curve.append((df.index[-1], self._balance))

        return self._compute_stats(equity_curve)

    @staticmethod
    def _close_cycle(cycle_fills, exit_reason):
        entries = [f for f in cycle_fills if f['side'] in ('BUY', 'SELL')]
        exit_fill = cycle_fills[-1]
        side = 'LONG' if entries and entries[0]['side'] == 'BUY' else 'SHORT'
        return {
            'side': side,
            'entries': len(entries),
            'avg_entry': (sum(e['price'] * e['qty'] for e in entries) / sum(e['qty'] for e in entries))
                         if entries else exit_fill['price'],
            'exit_price': exit_fill['price'],
            'exit_reason': exit_reason,
            # exit_fill['pnl'] only nets off the EXIT commission (that's all
            # _close_position needs, since entry commissions were already
            # subtracted from balance when each entry filled). For honest
            # cycle-level reporting (total_pnl, win/loss, profit_factor) we
            # need the FULL net result, so subtract entry commissions here
            # too — this makes sum(cycle pnl) == final_capital - initial_capital.
            'pnl': exit_fill['pnl'] - sum(e['commission'] for e in entries),
            'commission': sum(f['commission'] for f in cycle_fills),
            'start_time': cycle_fills[0]['time'] if cycle_fills else None,
            'end_time': exit_fill['time'],
        }

    def _compute_stats(self, equity_curve):
        fills, cycles = self._fills, self._cycles
        eq_values = [e for _, e in equity_curve] or [self.initial_capital]
        final_equity = eq_values[-1]
        total_return = (final_equity - self.initial_capital) / self.initial_capital * 100

        peak = eq_values[0]
        max_dd = 0.0
        for eq in eq_values:
            peak = max(peak, eq)
            dd = (peak - eq) / peak * 100 if peak > 0 else 0
            max_dd = max(max_dd, dd)

        wins = [c for c in cycles if c['pnl'] > 0]
        losses = [c for c in cycles if c['pnl'] <= 0]
        win_rate = (len(wins) / len(cycles) * 100) if cycles else 0
        gross_profit = sum(c['pnl'] for c in wins)
        gross_loss = abs(sum(c['pnl'] for c in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        def count_reason(r):
            return len([c for c in cycles if c['exit_reason'] == r])

        return {
            'initial_capital': self.initial_capital,
            'final_capital': round(final_equity, 2),
            'total_return_pct': round(total_return, 2),
            'max_drawdown_pct': round(max_dd, 2),
            'total_cycles': len(cycles),
            'tp_closes': count_reason('TP'),
            'stop_loss_closes': count_reason('STOP_LOSS'),
            'liquidations': count_reason('LIQUIDATION'),
            'forced_closes_at_end': count_reason('END_OF_DATA'),
            'buy_fills': len([f for f in fills if f['side'] == 'BUY']),
            'sell_fills': len([f for f in fills if f['side'] == 'SELL']),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': round(win_rate, 1),
            'avg_pnl_per_cycle': round(np.mean([c['pnl'] for c in cycles]), 2) if cycles else 0,
            'total_pnl': round(sum(c['pnl'] for c in cycles), 2),
            'total_commission': round(sum(f['commission'] for f in fills), 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else profit_factor,
            'cycles': cycles,
            'fills': fills,
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Grid Trading Backtester')
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--interval', default='1h')
    parser.add_argument('--start', default='2024-01')
    parser.add_argument('--end', default='2025-06')
    parser.add_argument('--n-levels', type=int, default=10)
    parser.add_argument('--proportion', type=float, default=1.5, help='Grid spacing in %%')
    parser.add_argument('--volume', type=float, default=0.05)
    parser.add_argument('--tp', type=float, default=3.0, help='Take-profit, %% ROI on margin used')
    parser.add_argument('--stop-loss', type=float, default=None, help='Stop-loss, %% ROI loss on margin used (default: disabled)')
    parser.add_argument('--leverage', type=int, default=1)
    parser.add_argument('--decimals', type=int, default=1, help='Price rounding precision')
    parser.add_argument('--capital', type=float, default=10000)
    parser.add_argument('--commission', type=float, default=0.04, help='Commission in %%')
    parser.add_argument('--slippage', type=float, default=0.05, help='Slippage in %%')
    args = parser.parse_args()

    print(f"\nGrid Trading Backtester — {args.symbol} {args.interval}")
    print(f"{'='*60}")
    print(f"Period:      {args.start} — {args.end}")
    print(f"Grid:        {args.n_levels} levels/side, {args.proportion}% spacing")
    print(f"Volume:      {args.volume} per level")
    print(f"TP:          {args.tp}% ROI on margin")
    print(f"Stop-loss:   {(str(args.stop_loss) + '% ROI on margin') if args.stop_loss else 'disabled'}")
    print(f"Leverage:    {args.leverage}x")
    print(f"Commission:  {args.commission}%   Slippage: {args.slippage}%")
    print(f"Capital:     ${args.capital:,.0f}")
    print()

    df = download_klines(args.symbol, args.interval, args.start, args.end)
    if len(df) == 0:
        print("No data loaded. Check symbol/interval/dates.")
        return
    print(f"Loaded {len(df)} candles: {df.index[0]} — {df.index[-1]}")

    cfg = GridConfig(
        symbol=args.symbol, n_levels=args.n_levels, proportion=args.proportion,
        volume=args.volume, tp_pct=args.tp, leverage=args.leverage,
        price_decimals=args.decimals, stop_loss_pct=args.stop_loss,
    )
    bt = GridBacktester(
        cfg, initial_capital=args.capital,
        commission_rate=args.commission / 100, slippage_rate=args.slippage / 100,
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
    print(f"  Completed Cycles:   {stats['total_cycles']}")
    print(f"    - closed by TP:       {stats['tp_closes']}")
    print(f"    - closed by stop-loss:{stats['stop_loss_closes']}")
    print(f"    - liquidated:         {stats['liquidations']}")
    print(f"    - forced at data end: {stats['forced_closes_at_end']}")
    print(f"  Wins / Losses:      {stats['wins']} / {stats['losses']}")
    print(f"  Win Rate:           {stats['win_rate']:.1f}%")
    print(f"  Profit Factor:      {stats['profit_factor']}")
    print(f"  Avg PnL/cycle:      ${stats['avg_pnl_per_cycle']:.2f}")
    print(f"  Total PnL:          ${stats['total_pnl']:.2f}")
    print(f"  Total Commission:   ${stats['total_commission']:.2f}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
