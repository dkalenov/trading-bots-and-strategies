"""Deep analysis of strategy viability — trade distribution, edge decay, regime dependency."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_DIR = Path(__file__).resolve().parent / "results" / "autonomous_4h_20250101_20260418_manual12"
RESULTS_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DIR


def main() -> None:
    trades = pd.read_csv(RESULTS_DIR / "trades.csv", parse_dates=["signal_time", "entry_time", "exit_time"])
    equity = pd.read_csv(RESULTS_DIR / "equity_curve.csv", parse_dates=["timestamp"])

    print("=" * 80)
    print("STRATEGY VIABILITY DEEP ANALYSIS — manual12 universe")
    print("=" * 80)

    # 1. Basic stats
    total = len(trades)
    wins = trades[trades["gross_net_pnl"] > 0]
    losses = trades[trades["gross_net_pnl"] <= 0]
    print(f"\nTotal trades: {total}")
    print(f"  Wins: {len(wins)} ({len(wins)/total*100:.1f}%)")
    print(f"  Losses: {len(losses)} ({len(losses)/total*100:.1f}%)")
    print(f"  Avg win: ${wins['gross_net_pnl'].mean():.2f}")
    print(f"  Avg loss: ${losses['gross_net_pnl'].mean():.2f}")
    print(f"  Win/Loss ratio: {abs(wins['gross_net_pnl'].mean() / losses['gross_net_pnl'].mean()):.2f}")
    print(f"  Expectancy per trade: ${trades['gross_net_pnl'].mean():.2f}")

    # 2. R-multiple distribution
    print(f"\n--- R-Multiple Distribution ---")
    r = trades["r_multiple"]
    print(f"  Mean R: {r.mean():.3f}")
    print(f"  Median R: {r.median():.3f}")
    print(f"  Std R: {r.std():.3f}")
    print(f"  Skew: {r.skew():.3f}")
    print(f"  R > 2: {(r > 2).sum()} trades ({(r > 2).mean()*100:.1f}%)")
    print(f"  R > 4: {(r > 4).sum()} trades ({(r > 4).mean()*100:.1f}%)")
    print(f"  R < -1: {(r < -1).sum()} trades ({(r < -1).mean()*100:.1f}%)")

    # 3. Exit reason breakdown
    print(f"\n--- Exit Reason Analysis ---")
    exit_stats = trades.groupby("exit_reason").agg(
        count=("gross_net_pnl", "count"),
        avg_pnl=("gross_net_pnl", "mean"),
        total_pnl=("gross_net_pnl", "sum"),
        avg_r=("r_multiple", "mean"),
        win_rate=("gross_net_pnl", lambda x: (x > 0).mean() * 100),
    ).sort_values("count", ascending=False)
    print(exit_stats.to_string())

    # 4. Direction analysis
    print(f"\n--- Direction Analysis ---")
    for direction in ["LONG", "SHORT"]:
        d = trades[trades["direction"] == direction]
        d_wins = d[d["gross_net_pnl"] > 0]
        print(f"\n  {direction}: {len(d)} trades, WR: {len(d_wins)/len(d)*100:.1f}%, "
              f"Avg PnL: ${d['gross_net_pnl'].mean():.2f}, "
              f"Total PnL: ${d['gross_net_pnl'].sum():.2f}, "
              f"Avg R: {d['r_multiple'].mean():.3f}")

    # 5. Quarterly breakdown
    print(f"\n--- Quarterly Performance ---")
    trades["quarter"] = trades["entry_time"].dt.to_period("Q")
    for q, group in trades.groupby("quarter"):
        total_pnl = group["gross_net_pnl"].sum()
        wr = (group["gross_net_pnl"] > 0).mean() * 100
        print(f"  {q}: {len(group):3d} trades, WR: {wr:5.1f}%, PnL: ${total_pnl:+8.2f}, "
              f"Avg R: {group['r_multiple'].mean():+.3f}")

    # 6. Monthly breakdown
    print(f"\n--- Monthly Performance ---")
    trades["month"] = trades["entry_time"].dt.to_period("M")
    for m, group in trades.groupby("month"):
        total_pnl = group["gross_net_pnl"].sum()
        wr = (group["gross_net_pnl"] > 0).mean() * 100
        print(f"  {m}: {len(group):3d} trades, WR: {wr:5.1f}%, PnL: ${total_pnl:+8.2f}")

    # 7. Consecutive losses analysis
    print(f"\n--- Consecutive Losses Analysis ---")
    is_loss = (trades["gross_net_pnl"] <= 0).values
    max_consec = 0
    current = 0
    consec_streaks = []
    for loss in is_loss:
        if loss:
            current += 1
        else:
            if current > 0:
                consec_streaks.append(current)
            current = 0
    if current > 0:
        consec_streaks.append(current)
    print(f"  Max consecutive losses: {max(consec_streaks) if consec_streaks else 0}")
    print(f"  Avg losing streak: {np.mean(consec_streaks):.1f}")
    print(f"  Losing streaks > 5: {sum(1 for s in consec_streaks if s > 5)}")
    print(f"  Losing streaks > 10: {sum(1 for s in consec_streaks if s > 10)}")

    # 8. Cluster analysis — how many signals fire together
    print(f"\n--- Signal Clustering ---")
    signal_counts = trades.groupby("signal_time").size()
    print(f"  Unique signal bars with entries: {len(signal_counts)}")
    print(f"  Avg entries per signal bar: {signal_counts.mean():.1f}")
    print(f"  Max entries per signal bar: {signal_counts.max()}")
    print(f"  Signal bars with 4-5 entries: {(signal_counts >= 4).sum()}")

    # 9. Correlation of PnL with entries (are clustered entries losing together?)
    print(f"\n--- Clustered Entry Correlation ---")
    multi_entry = trades.groupby("signal_time").filter(lambda x: len(x) >= 3)
    if len(multi_entry):
        multi_pnl = multi_entry.groupby("signal_time")["gross_net_pnl"].agg(["sum", "count", "mean"])
        multi_losses = multi_pnl[multi_pnl["sum"] < 0]
        print(f"  Clustered entries (3+): {len(multi_pnl)} groups, {len(multi_entry)} trades")
        print(f"  Groups with negative total PnL: {len(multi_losses)}/{len(multi_pnl)} ({len(multi_losses)/len(multi_pnl)*100:.0f}%)")
        print(f"  Avg group PnL: ${multi_pnl['sum'].mean():.2f}")
        if len(multi_losses):
            print(f"  Avg loss per losing cluster: ${multi_losses['sum'].mean():.2f}")

    # 10. TP1 hit analysis
    print(f"\n--- TP1 Hit Analysis ---")
    tp1_hit = trades[trades["tp1_hit"]]
    tp1_miss = trades[~trades["tp1_hit"]]
    print(f"  TP1 hit: {len(tp1_hit)} ({len(tp1_hit)/total*100:.1f}%)")
    print(f"  After TP1: avg final R: {tp1_hit['r_multiple'].mean():.3f}, total PnL: ${tp1_hit['gross_net_pnl'].sum():.2f}")
    print(f"  No TP1: avg final R: {tp1_miss['r_multiple'].mean():.3f}, total PnL: ${tp1_miss['gross_net_pnl'].sum():.2f}")

    # 11. Symbol contribution
    print(f"\n--- Symbol Contribution ---")
    sym_stats = trades.groupby("symbol").agg(
        count=("gross_net_pnl", "count"),
        total_pnl=("gross_net_pnl", "sum"),
        avg_r=("r_multiple", "mean"),
        win_rate=("gross_net_pnl", lambda x: (x > 0).mean() * 100),
    ).sort_values("total_pnl", ascending=False)
    print(sym_stats.to_string())

    # 12. Drawdown analysis
    print(f"\n--- Drawdown Analysis ---")
    eq = equity["equity"].values
    peak = np.maximum.accumulate(eq)
    dd = eq / peak - 1.0
    print(f"  Max Drawdown: {dd.min()*100:.2f}%")
    # Time in drawdown
    in_dd = dd < -0.05
    pct_time_in_dd = in_dd.mean() * 100
    print(f"  Time in >5% drawdown: {pct_time_in_dd:.1f}%")
    in_dd_10 = dd < -0.10
    print(f"  Time in >10% drawdown: {in_dd_10.mean()*100:.1f}%")

    # 13. Edge ratio analysis
    print(f"\n--- Edge Ratio (MAE/MFE proxy) ---")
    # Use r_multiple as proxy
    big_wins = trades[trades["r_multiple"] > 2]
    small_wins = trades[(trades["r_multiple"] > 0) & (trades["r_multiple"] <= 1)]
    typical_loss_r = losses["r_multiple"].median()
    print(f"  Big wins (R>2): {len(big_wins)} ({len(big_wins)/total*100:.1f}%) — these drive profitability")
    print(f"  Small wins (0<R<=1): {len(small_wins)} ({len(small_wins)/total*100:.1f}%)")
    print(f"  Typical loss R: {typical_loss_r:.3f}")

    # 14. Profitability without top N trades
    print(f"\n--- Robustness: Profitability Without Top Trades ---")
    sorted_trades = trades.sort_values("gross_net_pnl", ascending=False)
    total_pnl_all = trades["gross_net_pnl"].sum()
    for n in [3, 5, 10]:
        pnl_without_top = sorted_trades.iloc[n:]["gross_net_pnl"].sum()
        print(f"  Without top {n}: ${pnl_without_top:+.2f} (vs ${total_pnl_all:+.2f} full)")

    # 15. Annualized metrics
    print(f"\n--- Annualized Metrics ---")
    days = (trades["exit_time"].max() - trades["entry_time"].min()).days
    total_return = total_pnl_all / 10000  # initial equity
    annual_return = (1 + total_return) ** (365 / days) - 1
    print(f"  Period: {days} days")
    print(f"  Total return: {total_return*100:.2f}%")
    print(f"  Annualized return: {annual_return*100:.2f}%")
    print(f"  Max DD: {dd.min()*100:.2f}%")
    print(f"  Return/MaxDD (Calmar-like): {abs(annual_return / dd.min()):.2f}")
    trades_per_year = total / (days / 365)
    print(f"  Trades/year: {trades_per_year:.0f}")


if __name__ == "__main__":
    main()
