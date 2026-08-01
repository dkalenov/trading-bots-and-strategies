"""
Robustness / out-of-sample checks for the candle pattern strategy.

The "optimized" parameters in the README were tuned and evaluated on a single
18-month BTCUSDT 1h dataset — no train/test split, no walk-forward, no other
symbol or timeframe. That's a real overfitting risk. This script runs the
same parameters against data they were NOT tuned on, so the numbers here are
a much more honest signal of whether the edge is real.

Usage:
    python validate.py --symbol BTCUSDT

Notes:
  - Requires internet access to Bybit's API (or pre-cached CSVs in ./klines).
  - This is not a proper walk-forward optimizer — it doesn't re-fit parameters
    per fold. It answers a narrower question: "do parameters chosen on one
    slice of data still work on a different slice/timeframe?" A negative
    result here is strong evidence against the strategy; a positive result
    is supportive but not proof the edge is real (both slices still involve
    the same asset in a similar overall market regime).
"""
import argparse
from backtest import Backtester, download_klines


def run(label, df, **kwargs):
    bt = Backtester(initial_capital=10000, risk_pct=0.01, leverage=10)
    stats, trades = bt.run(df.copy(), **kwargs)
    print(f"{label:42s} | Return: {stats['total_return_pct']:+8.2f}% | "
          f"Trades: {stats['total_trades']:5d} | WinRate: {stats['win_rate']:5.1f}% | "
          f"PF: {stats['profit_factor']:.2f} | MaxDD: {stats['max_drawdown_pct']:.2f}% | "
          f"Sharpe: {stats['sharpe_ratio']:.3f}")
    return stats


def main():
    p = argparse.ArgumentParser(description='Out-of-sample robustness checks')
    p.add_argument('--symbol', default='BTCUSDT')
    p.add_argument('--sl-atr', type=float, default=2.0)
    p.add_argument('--tp-atr', type=float, default=4.0)
    p.add_argument('--min-strength', type=float, default=1.3)
    args = p.parse_args()

    params = dict(sl_atr=args.sl_atr, tp_atr=args.tp_atr,
                  min_strength=args.min_strength,
                  use_trend_filter=True, min_atr_pct=0.3)

    print(f"Params under test: {params}\n")

    # 1) Time split within the original 1h dataset — sanity check the equity
    #    curve is reasonably consistent across the window, NOT a substitute
    #    for true out-of-sample (both halves were part of the tuning period).
    df1h = download_klines(args.symbol, '1h', '2024-01', '2025-06')
    mid = len(df1h) // 2
    first_half = df1h.iloc[:mid + 250]
    second_half = df1h.iloc[mid - 250:]
    print("=== Time-split within tuning period (not true OOS, just a consistency check) ===")
    run("First half", first_half, **params)
    run("Second half", second_half, **params)

    # 2) Different timeframe, same asset, overlapping period — closer to a
    #    real out-of-sample check since the parameters were never exposed to
    #    30m bar structure.
    print("\n=== Different timeframe (30m), same asset — real generalization check ===")
    try:
        df30 = download_klines(args.symbol, '30m', '2024-09', '2025-06')
        run("30m bars", df30, **params)
    except Exception as e:
        print(f"  Skipped (no cached data / no network): {e}")

    print("\nInterpretation: if the 30m result is sharply worse or negative while")
    print("the 1h result is strongly positive, that's evidence the parameters are")
    print("fitted to the specific 1h dataset rather than reflecting a robust edge.")
    print("Before trusting this strategy with real capital, also test on a period")
    print("NOT used to pick sl_atr/tp_atr/min_strength, and on a second symbol.")


if __name__ == '__main__':
    main()
