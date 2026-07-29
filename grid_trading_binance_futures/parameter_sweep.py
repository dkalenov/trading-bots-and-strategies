"""
Parameter sweep for the grid backtester.

This is deliberately NOT a search for "the best" parameters — it's a
systematic, reproducible scan of the parameter space on the same
BTCUSDT 1h data used elsewhere in this repo, so the honest shape of the
strategy's behaviour (including the combinations that lose money) is on
the record rather than cherry-picked.

Usage:
    python parameter_sweep.py --start 2022-01 --end 2025-06 --out sweep_full.csv
    python parameter_sweep.py --start 2025-01 --end 2025-06 --out sweep_2025h1.csv
"""

import argparse
import csv
import itertools
import time

from grid_strategy import GridConfig
from backtest import GridBacktester, download_klines


# (n_levels, proportion) pairs -> total grid width = n_levels * proportion %
GRID_WIDTHS = [
    (10, 1.5),   # 15% total width (repo default)
    (20, 1.0),   # 20%
    (15, 1.5),   # 22.5%
    (10, 2.5),   # 25%
    (15, 2.5),   # 37.5%
]
TP_VALUES = [2.0, 3.0, 5.0]
STOP_LOSS_VALUES = [None, 5.0, 10.0, 15.0, 20.0, 30.0]


def run_sweep(symbol, interval, start, end, capital, leverage, out_path):
    df = download_klines(symbol, interval, start, end)
    print(f"Loaded {len(df)} candles: {df.index[0]} — {df.index[-1]}")

    combos = list(itertools.product(GRID_WIDTHS, TP_VALUES, STOP_LOSS_VALUES))
    print(f"Running {len(combos)} combinations...")

    rows = []
    t0 = time.time()
    for i, ((n_levels, proportion), tp, sl) in enumerate(combos):
        cfg = GridConfig(
            symbol=symbol, n_levels=n_levels, proportion=proportion, volume=0.05,
            tp_pct=tp, leverage=leverage, price_decimals=2, stop_loss_pct=sl,
        )
        bt = GridBacktester(cfg, initial_capital=capital, commission_rate=0.0004, slippage_rate=0.0005)
        stats = bt.run(df)
        rows.append({
            'n_levels': n_levels,
            'proportion': proportion,
            'grid_width_pct': round(n_levels * proportion, 1),
            'tp_pct': tp,
            'stop_loss_pct': sl if sl is not None else 'none',
            'total_return_pct': stats['total_return_pct'],
            'max_drawdown_pct': stats['max_drawdown_pct'],
            'total_cycles': stats['total_cycles'],
            'win_rate': stats['win_rate'],
            'profit_factor': stats['profit_factor'],
            'liquidations': stats['liquidations'],
            'forced_closes_at_end': stats['forced_closes_at_end'],
        })
        if (i + 1) % 10 == 0:
            elapsed = time.time() - t0
            print(f"  {i+1}/{len(combos)} done ({elapsed:.0f}s elapsed)")

    rows.sort(key=lambda r: r['total_return_pct'], reverse=True)

    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    n_profitable = len([r for r in rows if r['total_return_pct'] > 0])
    print(f"\nWrote {len(rows)} results to {out_path}")
    print(f"Profitable combinations: {n_profitable}/{len(rows)}")
    print(f"\nTop 5 by return:")
    for r in rows[:5]:
        print(f"  n={r['n_levels']:2d} prop={r['proportion']:.1f}% width={r['grid_width_pct']:5.1f}% "
              f"tp={r['tp_pct']:.1f}% sl={r['stop_loss_pct']!s:5s} -> "
              f"return={r['total_return_pct']:+8.2f}%  maxDD={r['max_drawdown_pct']:6.2f}%  "
              f"winrate={r['win_rate']:.1f}%  cycles={r['total_cycles']}")
    print(f"\nBottom 5 by return:")
    for r in rows[-5:]:
        print(f"  n={r['n_levels']:2d} prop={r['proportion']:.1f}% width={r['grid_width_pct']:5.1f}% "
              f"tp={r['tp_pct']:.1f}% sl={r['stop_loss_pct']!s:5s} -> "
              f"return={r['total_return_pct']:+8.2f}%  maxDD={r['max_drawdown_pct']:6.2f}%  "
              f"winrate={r['win_rate']:.1f}%  cycles={r['total_cycles']}")

    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parameter sweep for the grid backtester')
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--interval', default='1h')
    parser.add_argument('--start', default='2022-01')
    parser.add_argument('--end', default='2025-06')
    parser.add_argument('--capital', type=float, default=10000)
    parser.add_argument('--leverage', type=int, default=1)
    parser.add_argument('--out', default='sweep_results.csv')
    args = parser.parse_args()

    run_sweep(args.symbol, args.interval, args.start, args.end, args.capital, args.leverage, args.out)
