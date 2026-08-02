#!/usr/bin/env python3
"""
Run the full pipeline (download -> signals -> risk-managed backtest)
across many symbols and produce one aggregated comparison report.

Each symbol is backtested completely independently — same starting
capital, own trade log, own charts — this is NOT a combined portfolio
equity curve. It's meant to answer "does this hold up across
different markets, or does it only work on the one pair I tuned it
on", which is the right question to ask before trusting a strategy.

Examples
--------
    python run_multi_backtest.py
    python run_multi_backtest.py --symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT \
        --interval 4h --start 2023-01 --end 2025-02
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

import pandas as pd  # noqa: E402

from data_loader import load_or_download, load_multi_symbol_csv, iter_symbols  # noqa: E402
from fractals import generate_signals  # noqa: E402
from backtest import run_backtest  # noqa: E402
from plotting import plot_price_with_signals, plot_equity_curve  # noqa: E402

DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,LINKUSDT"

INTERVAL_TO_PERIODS_PER_YEAR = {
    "1m": 60 * 24 * 365,
    "5m": 12 * 24 * 365,
    "15m": 4 * 24 * 365,
    "1h": 24 * 365,
    "4h": 6 * 365,
    "1d": 365,
}


def parse_args():
    p = argparse.ArgumentParser(description="Williams Fractal breakout — multi-symbol batch backtest")
    p.add_argument("--symbols", default=None,
                    help="comma-separated list, e.g. BTCUSDT,ETHUSDT. "
                         "With --input-csv and no --symbols, ALL symbols found in the file are used.")
    p.add_argument("--input-csv", default=None,
                    help="path to a combined CSV with columns date,open,high,low,close,volume,symbol "
                         "— skips downloading, uses this file for every symbol instead")
    p.add_argument("--max-symbols", type=int, default=None,
                    help="cap the number of symbols processed (useful with --input-csv and no --symbols)")
    p.add_argument("--interval", default="1h", choices=list(INTERVAL_TO_PERIODS_PER_YEAR))
    p.add_argument("--start", default="2024-11", help="YYYY-MM, inclusive (ignored with --input-csv)")
    p.add_argument("--end", default="2025-02", help="YYYY-MM, inclusive (ignored with --input-csv)")
    p.add_argument("--fractal-n", type=int, default=2)
    p.add_argument("--capital", type=float, default=10_000.0)
    p.add_argument("--fee", type=float, default=0.0004)
    p.add_argument("--risk-per-trade", type=float, default=0.01)
    p.add_argument("--stop-mode", default="structure", choices=["structure", "atr", "percent"])
    p.add_argument("--atr-period", type=int, default=14)
    p.add_argument("--atr-multiplier", type=float, default=1.5)
    p.add_argument("--stop-pct", type=float, default=0.02)
    p.add_argument("--reward-risk", type=float, default=2.0)
    p.add_argument("--max-leverage", type=float, default=1.0)
    p.add_argument("--no-plot", action="store_true")
    p.add_argument("--plot-top-n", type=int, default=5,
                    help="with --no-plot off and many symbols, only chart the N best and N worst by return "
                         "(charting all of them is slow and rarely useful) — 0 disables the cap")
    p.add_argument("--out-dir", default="output_batch")
    return p.parse_args()


def run_one(symbol: str, args, df: pd.DataFrame | None = None, make_plots: bool = True) -> dict:
    row = {"symbol": symbol}
    try:
        if df is None:
            df = load_or_download(symbol, args.interval, args.start, args.end)
    except Exception as e:
        row.update(status="data_error", error=str(e))
        return row

    row["num_candles"] = len(df)

    try:
        df = generate_signals(df, n=args.fractal_n)
    except Exception as e:
        row.update(status="signal_error", error=str(e))
        return row

    n_signals = int((df["signal"] != 0).sum())
    row["num_signals"] = n_signals
    if n_signals == 0:
        row["status"] = "no_signals"
        return row

    try:
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
    except Exception as e:
        row.update(status="backtest_error", error=str(e))
        traceback.print_exc()
        return row

    row["status"] = "ok"
    row.update(result.metrics)

    sym_dir = os.path.join(args.out_dir, symbol)
    os.makedirs(sym_dir, exist_ok=True)
    trades_df = result.trades_df()
    trades_df.to_csv(os.path.join(sym_dir, "trades.csv"), index=False)

    if not args.no_plot and make_plots:
        plot_price_with_signals(
            df, symbol, args.interval, os.path.join(sym_dir, "price_signals.png"), trades_df=trades_df
        )
        plot_equity_curve(result.equity_curve, os.path.join(sym_dir, "equity_curve.png"))

    return row


def write_markdown_report(summary: pd.DataFrame, args, out_path: str, table_cap: int = 30) -> None:
    ok = summary[summary["status"] == "ok"].copy()
    lines = [
        "# Batch backtest summary",
        "",
        f"Interval: `{args.interval}`  |  "
        + (f"Source: `{args.input_csv}`" if args.input_csv else f"Range: `{args.start}` .. `{args.end}`")
        + f"  |  risk/trade: `{args.risk_per_trade:.1%}`  |  stop mode: `{args.stop_mode}`  |  "
        f"reward:risk: `{args.reward_risk}`  |  max leverage: `{args.max_leverage}x`",
        "",
        f"{len(summary)} symbols processed, {len(ok)} produced a valid backtest. "
        f"Each symbol backtested independently with the same starting capital "
        f"(`{args.capital:,.0f}`) — not a combined portfolio.",
        "",
    ]

    cols = [
        ("symbol", "Symbol"),
        ("num_signals", "Signals"),
        ("num_trades", "Trades"),
        ("win_rate_pct", "Win %"),
        ("total_return_pct", "Return %"),
        ("max_drawdown_pct", "Max DD %"),
        ("sharpe_ratio", "Sharpe"),
        ("profit_factor", "Profit factor"),
        ("stop_outs", "Stops"),
        ("take_profits", "TPs"),
        ("signal_flips", "Flips"),
    ]

    def render_table(rows_df: pd.DataFrame) -> list[str]:
        out = ["| " + " | ".join(c[1] for c in cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
        for _, r in rows_df.iterrows():
            vals = []
            for key, _ in cols:
                v = r.get(key, "")
                if isinstance(v, float):
                    v = f"{v:.2f}"
                vals.append(str(v))
            out.append("| " + " | ".join(vals) + " |")
        return out

    if len(ok) == 0:
        lines.append("No symbol produced a valid backtest — see summary.csv for error details.")
    else:
        ranked = ok.sort_values("total_return_pct", ascending=False)

        lines += [
            "**Across all symbols:** "
            f"median return {ok['total_return_pct'].median():.2f}%, "
            f"mean return {ok['total_return_pct'].mean():.2f}%, "
            f"median max drawdown {ok['max_drawdown_pct'].median():.2f}%, "
            f"{int((ok['total_return_pct'] > 0).sum())}/{len(ok)} symbols finished positive "
            f"({100 * (ok['total_return_pct'] > 0).mean():.1f}%).",
            "",
        ]

        if len(ranked) <= table_cap:
            lines += render_table(ranked)
        else:
            half = table_cap // 2
            lines += [f"### Top {half} by return", ""]
            lines += render_table(ranked.head(half))
            lines += ["", f"### Bottom {half} by return", ""]
            lines += render_table(ranked.tail(half))
            lines += [
                "",
                f"_{len(ranked) - 2 * half} more symbols not shown here — full results for all "
                f"{len(ranked)} symbols are in `summary.csv`._",
            ]

    failed = summary[summary["status"] != "ok"]
    if len(failed) > 0:
        lines += ["", f"### Symbols skipped ({len(failed)})", ""]
        reason_counts = failed["status"].value_counts()
        for reason, count in reason_counts.items():
            lines.append(f"- {reason}: {count}")
        if len(failed) <= 20:
            for _, r in failed.iterrows():
                err = r.get("error")
                err_str = f": {err}" if isinstance(err, str) and err else ""
                lines.append(f"  - `{r['symbol']}` — {r['status']}{err_str}")

    lines += [
        "",
        "---",
        "A strategy that only performs on a handful out of many symbols is much",
        "more likely to be overfit / lucky than one that holds up broadly. Treat a",
        "short list of winners among many symbols with real suspicion.",
    ]

    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    combined_df = None
    if args.input_csv:
        print(f"[batch] loading {args.input_csv} ...")
        combined_df = load_multi_symbol_csv(args.input_csv)
        available = sorted(combined_df["symbol"].unique().tolist())
        print(f"[batch] {len(available)} symbols found in file, "
              f"{len(combined_df)} rows total, "
              f"{combined_df['date'].min()} .. {combined_df['date'].max()}")
        if args.symbols:
            symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
            missing = [s for s in symbols if s not in available]
            if missing:
                print(f"[batch] warning: not found in file, skipping: {missing}")
            symbols = [s for s in symbols if s in available]
        else:
            symbols = available
    else:
        symbols = [s.strip().upper() for s in (args.symbols or DEFAULT_SYMBOLS).split(",") if s.strip()]

    if args.max_symbols:
        symbols = symbols[: args.max_symbols]

    print(f"[batch] processing {len(symbols)} symbols ...")

    rows = []
    for idx, symbol in enumerate(symbols, 1):
        sub_df = None
        if combined_df is not None:
            sub_df = combined_df[combined_df["symbol"] == symbol].drop(columns=["symbol"]).reset_index(drop=True)

        # first pass: never plot here — plotting happens in a second pass
        # for only the most interesting symbols (see below), so a run
        # across hundreds of symbols doesn't spend most of its time
        # rendering charts nobody will look at.
        row = run_one(symbol, args, df=sub_df, make_plots=False)
        rows.append(row)
        status = row.get("status")
        if status == "ok":
            print(f"[batch] ({idx}/{len(symbols)}) {symbol}: return {row['total_return_pct']:.2f}%, "
                  f"max DD {row['max_drawdown_pct']:.2f}%, {row['num_trades']} trades")
        else:
            print(f"[batch] ({idx}/{len(symbols)}) {symbol}: {status} {row.get('error', '')}")

    summary = pd.DataFrame(rows)

    # second pass: (re)generate charts only for the top/bottom N by
    # return, plus everything if the whole batch is small anyway.
    if not args.no_plot:
        ok = summary[summary["status"] == "ok"]
        if args.plot_top_n and len(ok) > 2 * args.plot_top_n:
            ranked = ok.sort_values("total_return_pct", ascending=False)
            to_plot = pd.concat([ranked.head(args.plot_top_n), ranked.tail(args.plot_top_n)])["symbol"].tolist()
            print(f"[batch] charting top/bottom {args.plot_top_n} by return ({len(to_plot)} symbols) ...")
        else:
            to_plot = ok["symbol"].tolist()

        for symbol in to_plot:
            sub_df = None
            if combined_df is not None:
                sub_df = combined_df[combined_df["symbol"] == symbol].drop(columns=["symbol"]).reset_index(drop=True)
            run_one(symbol, args, df=sub_df, make_plots=True)

    summary_path = os.path.join(args.out_dir, "summary.csv")
    summary.to_csv(summary_path, index=False)

    report_path = os.path.join(args.out_dir, "SUMMARY.md")
    write_markdown_report(summary, args, report_path)

    print()
    print(f"[batch] {len(symbols)} symbols processed, "
          f"{(summary['status'] == 'ok').sum()} produced a valid backtest")
    print(f"[batch] summary table -> {summary_path}")
    print(f"[batch] markdown report -> {report_path}")


if __name__ == "__main__":
    main()
