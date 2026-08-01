#!/usr/bin/env python3
"""
End-to-end runner: download data -> detect fractals -> generate signals
-> backtest -> print report -> save charts.

Examples
--------
    python run_backtest.py
    python run_backtest.py --symbol ETHUSDT --interval 15m --start 2024-06 --end 2025-01
    python run_backtest.py --csv my_data.csv --no-plot
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from data_loader import load_or_download  # noqa: E402
from fractals import generate_signals  # noqa: E402
from backtest import run_backtest, print_report  # noqa: E402
from plotting import plot_price_with_signals, plot_equity_curve  # noqa: E402


INTERVAL_TO_PERIODS_PER_YEAR = {
    "1m": 60 * 24 * 365,
    "5m": 12 * 24 * 365,
    "15m": 4 * 24 * 365,
    "1h": 24 * 365,
    "4h": 6 * 365,
    "1d": 365,
}


def parse_args():
    p = argparse.ArgumentParser(description="Williams Fractal breakout — backtest runner")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="1h", choices=list(INTERVAL_TO_PERIODS_PER_YEAR))
    p.add_argument("--start", default="2024-11", help="YYYY-MM, inclusive")
    p.add_argument("--end", default="2025-02", help="YYYY-MM, inclusive")
    p.add_argument("--csv", default=None, help="use a local CSV instead of downloading")
    p.add_argument("--fractal-n", type=int, default=2, help="bars on each side of the pivot (2 = classic 5-bar fractal)")
    p.add_argument("--capital", type=float, default=10_000.0)
    p.add_argument("--fee", type=float, default=0.0004, help="per-side fee, e.g. 0.0004 = 4bps")

    p.add_argument("--risk-per-trade", type=float, default=0.01,
                    help="fraction of equity risked per trade if the stop is hit, e.g. 0.01 = 1%%")
    p.add_argument("--stop-mode", default="structure", choices=["structure", "atr", "percent"],
                    help="how the stop-loss is placed (see README)")
    p.add_argument("--atr-period", type=int, default=14)
    p.add_argument("--atr-multiplier", type=float, default=1.5)
    p.add_argument("--stop-pct", type=float, default=0.02, help="used only when --stop-mode percent")
    p.add_argument("--reward-risk", type=float, default=2.0,
                    help="take-profit distance as a multiple of the stop distance")
    p.add_argument("--max-leverage", type=float, default=1.0,
                    help="cap on notional exposure as a fraction of equity (1.0 = no leverage)")

    p.add_argument("--no-plot", action="store_true")
    p.add_argument("--out-dir", default="output")
    return p.parse_args()


def main():
    args = parse_args()

    if args.csv:
        import pandas as pd
        df = pd.read_csv(args.csv, parse_dates=["date"])
    else:
        df = load_or_download(args.symbol, args.interval, args.start, args.end)

    print(f"[run_backtest] {len(df)} candles loaded "
          f"({df['date'].iloc[0]} .. {df['date'].iloc[-1]})")

    df = generate_signals(df, n=args.fractal_n)
    n_signals = int((df["signal"] != 0).sum())
    print(f"[run_backtest] {n_signals} signals generated "
          f"({(df['signal'] == 1).sum()} long, {(df['signal'] == -1).sum()} short)")

    if n_signals == 0:
        print("[run_backtest] No signals in this range — nothing to backtest.")
        return

    result = run_backtest(
        df,
        initial_capital=args.capital,
        fee_rate=args.fee,
        periods_per_year=INTERVAL_TO_PERIODS_PER_YEAR[args.interval],
        risk_per_trade=args.risk_per_trade,
        stop_mode=args.stop_mode,
        atr_period=args.atr_period,
        atr_multiplier=args.atr_multiplier,
        stop_pct=args.stop_pct,
        reward_risk_ratio=args.reward_risk,
        max_leverage=args.max_leverage,
    )
    print_report(result)

    trades_path = f"{args.out_dir}/trades_{args.symbol}_{args.interval}.csv"
    import os
    os.makedirs(args.out_dir, exist_ok=True)
    trades_df = result.trades_df()
    trades_df.to_csv(trades_path, index=False)
    print(f"[run_backtest] trade log saved -> {trades_path}")

    if not args.no_plot:
        price_path = f"{args.out_dir}/price_signals_{args.symbol}_{args.interval}.png"
        equity_path = f"{args.out_dir}/equity_curve_{args.symbol}_{args.interval}.png"
        plot_price_with_signals(df, args.symbol, args.interval, price_path, trades_df=trades_df)
        plot_equity_curve(result.equity_curve, equity_path)
        print(f"[run_backtest] charts saved -> {price_path}, {equity_path}")


if __name__ == "__main__":
    main()
