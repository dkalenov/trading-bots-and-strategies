"""
Usage:
    python -m backtest.run_backtest --klines path/to/klines_4h.csv \
        --signals path/to/tradingview_signals.csv \
        --out-dir results/

Prints the same honest, unfiltered metrics table shown in README.md and
writes trades.csv + per_symbol_summary.csv + summary.json to --out-dir.
"""
from __future__ import annotations
import argparse
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from backtest.engine import load_klines, load_signals, run_backtest, BacktestConfig


def compute_metrics(trades: pd.DataFrame, start_capital: float, order_size_usd: float) -> dict:
    if trades.empty:
        return {"total_trades": 0}

    trades = trades.sort_values("entry_time").reset_index(drop=True)
    wins = trades["pnl_usd"] > 0
    gross_profit = trades.loc[wins, "pnl_usd"].sum()
    gross_loss = -trades.loc[~wins, "pnl_usd"].sum()

    equity = start_capital + trades["pnl_usd"].cumsum()
    running_max = equity.cummax()
    max_dd_pct = ((equity - running_max) / running_max * 100).min()

    notional_total = len(trades) * order_size_usd

    return {
        "total_trades": int(len(trades)),
        "win_rate_pct": round(100 * wins.mean(), 2),
        "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss > 0 else None,
        "total_pnl_usd": round(trades["pnl_usd"].sum(), 2),
        "return_on_notional_pct": round(100 * trades["pnl_usd"].sum() / notional_total, 3),
        "start_capital_usd": start_capital,
        "final_equity_usd": round(equity.iloc[-1], 2),
        "return_on_start_capital_pct": round(100 * (equity.iloc[-1] - start_capital) / start_capital, 3),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "profitable_symbols": int(trades.groupby("symbol")["pnl_usd"].sum().gt(0).sum()),
        "total_symbols_traded": int(trades["symbol"].nunique()),
        "period_start": str(trades["entry_time"].min()),
        "period_end": str(trades["entry_time"].max()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--klines", required=True)
    ap.add_argument("--signals", required=True)
    ap.add_argument("--out-dir", default="results")
    ap.add_argument("--start-capital", type=float, default=10000.0)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    cfg = BacktestConfig()

    klines = load_klines(args.klines)
    signals = load_signals(args.signals, cfg.signal_clean_start)

    trades, open_at_end = run_backtest(klines, signals, cfg)
    trades.to_csv(os.path.join(args.out_dir, "trades.csv"), index=False)

    per_symbol = trades.groupby("symbol").agg(
        trades=("pnl_usd", "size"),
        win_rate_pct=("pnl_usd", lambda x: round(100 * (x > 0).mean(), 2)),
        total_pnl_usd=("pnl_usd", lambda x: round(x.sum(), 2)),
    ).sort_values("total_pnl_usd", ascending=False)
    per_symbol.to_csv(os.path.join(args.out_dir, "per_symbol_summary.csv"))

    metrics = compute_metrics(trades, args.start_capital, cfg.strategy.order_size_usd)
    metrics["open_positions_unresolved_at_end"] = len(open_at_end)
    with open(os.path.join(args.out_dir, "summary.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
