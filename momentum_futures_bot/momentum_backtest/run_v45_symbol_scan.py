"""
V4.5 Per-Symbol Scanner — Baseline vs D_chop on ALL cached symbols
==================================================================

Runs each symbol independently (single-symbol backtest) with:
  A) Baseline config
  B) D_chop config (btc_choppy_threshold_mult=1.3, btc_choppy_adx=20)

Outputs:
  - Per-symbol CSV with L/S breakdown for both configs
  - Rankings by OOS PF, balance score, and combined score
  - Portfolio candidate list (symbols that are profitable on both sides)
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))

import math
import time
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import binance
except ImportError:
    import binance_connector as binance  # type: ignore

from momentum_core.backtester import BacktestResult, RegimeBreakoutBacktester
from momentum_core.strategy import StrategyConfig, create_strategy
from momentum_backtest.research_utils import (
    build_is_oos_split,
    build_strategy_config,
    download_market_data,
    parse_date,
)

# Import from our v4.5 test module
from run_v45_balance_test import (
    BalancedMomentumStrategy,
    direction_breakdown,
)


# ── Config ──────────────────────────────────────────────────────────────

COMMON = {
    "signal_mode": "structured",
    "timeframe": "4h",
    "struct_trend_min": 0.50,
    "struct_momentum_min": 0.40,
    "struct_timing_min": 0.25,
    "struct_composite_min": 0.25,
    "struct_weight_profile": "v41",
    "stop_atr": 1.5,
    "take1_atr": 2.0,
    "take2_atr": 4.0,
    "partial_exit_pct": 0.1,
    "trail_after_tp1": False,
    "risk_per_trade": 0.01,
    "max_positions": 1,           # 1 position per symbol (single-symbol test)
    "max_new_entries_per_bar": 1,
    "symbol_cooldown_bars": 3,
    "max_entry_gap_atr": 2.0,
    "funding_mode": "fixed",
    "use_stale_exit": False,
    "use_regime_exit": False,
    "use_ema_trail": False,
    "use_time_exit": False,
    "use_adaptive_sizing": True,
    "adaptive_min_mult": 0.5,
    "adaptive_max_mult": 1.5,
}

INITIAL_EQUITY = 100.0

CONFIGS = {
    "baseline": {"overrides": {}, "use_3zone_btc": False},
    "d_chop":   {"overrides": {"btc_choppy_threshold_mult": 1.3, "btc_choppy_adx": 20.0}, "use_3zone_btc": False},
}


def run_single_symbol(
    symbol: str,
    symbol_frame: pd.DataFrame,
    btc_frame: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    config_name: str,
) -> BacktestResult:
    """Run backtest for a single symbol with the given config."""
    conf = CONFIGS[config_name]
    overrides = dict(COMMON)
    overrides.update(conf["overrides"])
    cfg = build_strategy_config(overrides)

    strategy = BalancedMomentumStrategy(
        config=cfg,
        use_3zone_btc=conf["use_3zone_btc"],
    )
    bt = RegimeBreakoutBacktester(strategy=strategy, initial_equity=INITIAL_EQUITY)
    return bt.run(
        symbol_frames={symbol: symbol_frame},
        btc_frame=btc_frame,
        start=start,
        end=end,
    )


def main():
    output_dir = Path("results/v45_per_symbol_scan")
    output_dir.mkdir(parents=True, exist_ok=True)

    full_start = parse_date("2024-01-01")
    full_end = parse_date("2026-06-05")
    is_start, is_end, oos_start, oos_end = build_is_oos_split(
        start=full_start, end=full_end, is_months=18, oos_months=99,
    )

    print("=" * 120)
    print("V4.5 PER-SYMBOL SCANNER -- Baseline vs D_chop on ALL cached symbols")
    print(f"IS:  {is_start.date()} -> {is_end.date()}")
    print(f"OOS: {oos_start.date()} -> {oos_end.date()}")
    print("=" * 120)

    # ── Load all cached symbols ──────────────────────────────────────
    cache_dir = Path("cache/4h")
    if not cache_dir.exists():
        print("ERROR: cache/4h not found")
        return

    # Find all unique symbols in cache
    all_symbols = set()
    for csv_file in cache_dir.glob("*.csv"):
        sym = csv_file.stem.split("_4h")[0]
        if sym and sym != "BTCUSDT":
            all_symbols.add(sym)
    all_symbols = sorted(all_symbols)
    print(f"\nFound {len(all_symbols)} symbols in cache")

    # Load BTC data from cache
    print("Loading BTC data...")
    warmup = full_start - timedelta(days=90)

    client = binance.Futures(asynced=False, testnet=False)
    btc_4h, sym_4h, skipped = download_market_data(
        client=client,
        symbols=["BTCUSDT"],
        interval="4h",
        history_start=warmup,
        end=full_end,
        cache_dir=cache_dir,
        min_history_cutoff=full_start - timedelta(days=365),
        min_bars_in_range=50,
        range_start=full_start,
        progress_label="btc",
        log_interval_seconds=999,
    )
    print(f"  BTC loaded: {len(btc_4h)} bars")

    # Load all symbol data
    print(f"Loading {len(all_symbols)} symbol datasets...")
    _, sym_all, sym_skipped = download_market_data(
        client=client,
        symbols=all_symbols,
        interval="4h",
        history_start=warmup,
        end=full_end,
        cache_dir=cache_dir,
        min_history_cutoff=full_start - timedelta(days=365),
        min_bars_in_range=100,
        range_start=full_start,
        progress_label="symbols",
        log_interval_seconds=30.0,
    )
    print(f"  Loaded: {len(sym_all)} symbols, skipped: {len(sym_skipped)}")

    # ── Run per-symbol tests ─────────────────────────────────────────
    results_rows: list[dict] = []
    total = len(sym_all)
    wall_start = time.time()

    for idx, (symbol, frame) in enumerate(sorted(sym_all.items()), 1):
        for config_name in ["baseline", "d_chop"]:
            for period, p_start, p_end in [("IS", is_start, is_end), ("OOS", oos_start, oos_end)]:
                try:
                    result = run_single_symbol(
                        symbol=symbol,
                        symbol_frame=frame,
                        btc_frame=btc_4h,
                        start=p_start,
                        end=p_end,
                        config_name=config_name,
                    )
                    s = result.summary
                    bd = direction_breakdown(result)

                    l_pf = min(bd["long_pf"], 5.0) if bd["long_trades"] > 2 else 0.0
                    s_pf = min(bd["short_pf"], 5.0) if bd["short_trades"] > 2 else 0.0
                    balance_score = round(math.sqrt(l_pf * s_pf), 3) if l_pf > 0 and s_pf > 0 else 0.0

                    row = {
                        "symbol": symbol,
                        "config": config_name,
                        "period": period,
                        "trades": s.get("closed_trades", 0),
                        "pf": round(s.get("profit_factor", 0) or 0, 3),
                        "ret": round(s.get("total_return_pct", 0), 1),
                        "wr": round(s.get("win_rate_pct", 0), 1),
                        "dd": round(s.get("max_drawdown_pct", 0), 1),
                        "avg_r": round(s.get("avg_r_multiple", 0), 3),
                        "balance_score": balance_score,
                        **bd,
                    }
                except Exception as e:
                    row = {
                        "symbol": symbol, "config": config_name, "period": period,
                        "trades": 0, "pf": 0, "ret": 0, "wr": 0, "dd": 0,
                        "avg_r": 0, "balance_score": 0,
                        "long_trades": 0, "short_trades": 0,
                        "long_pnl": 0, "short_pnl": 0,
                        "long_wr": 0, "short_wr": 0,
                        "long_pf": 0, "short_pf": 0,
                        "long_avg_r": 0, "short_avg_r": 0,
                        "max_consec_losses": 0,
                    }
                results_rows.append(row)

        elapsed = time.time() - wall_start
        eta = elapsed / idx * (total - idx)

        # Quick status for this symbol (OOS baseline)
        oos_bl = next((r for r in results_rows if r["symbol"] == symbol and r["config"] == "baseline" and r["period"] == "OOS"), None)
        oos_dc = next((r for r in results_rows if r["symbol"] == symbol and r["config"] == "d_chop" and r["period"] == "OOS"), None)
        if oos_bl and oos_dc:
            bl_pf = oos_bl["pf"]
            dc_pf = oos_dc["pf"]
            delta = dc_pf - bl_pf
            mark = ""
            if oos_bl["long_pnl"] > 0 and oos_bl["short_pnl"] > 0:
                mark = " [BAL]"
            print(
                f"  [{idx:3d}/{total}] {symbol:18s} "
                f"BL: PF={bl_pf:5.3f} L${oos_bl['long_pnl']:+6.1f} S${oos_bl['short_pnl']:+6.1f} "
                f"| DC: PF={dc_pf:5.3f} d={delta:+.3f}{mark}"
                f"  (ETA {eta/60:.0f}m)"
            )

    # ── Save full results ────────────────────────────────────────────
    df = pd.DataFrame(results_rows)
    csv_path = output_dir / "per_symbol_scan_results.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nFull CSV: {csv_path}")

    # ── Analysis: OOS rankings ───────────────────────────────────────
    oos_bl = df[(df["config"] == "baseline") & (df["period"] == "OOS")].copy()
    oos_dc = df[(df["config"] == "d_chop") & (df["period"] == "OOS")].copy()

    # Merge baseline and d_chop OOS
    merged = oos_bl[["symbol", "pf", "ret", "dd", "trades", "long_pnl", "short_pnl",
                      "long_pf", "short_pf", "long_trades", "short_trades",
                      "balance_score", "wr", "avg_r", "max_consec_losses"]].copy()
    merged.columns = ["symbol"] + [f"bl_{c}" for c in merged.columns[1:]]

    dc_cols = oos_dc[["symbol", "pf", "ret", "dd", "trades", "long_pnl", "short_pnl",
                       "long_pf", "short_pf", "long_trades", "short_trades",
                       "balance_score", "wr", "avg_r", "max_consec_losses"]].copy()
    dc_cols.columns = ["symbol"] + [f"dc_{c}" for c in dc_cols.columns[1:]]

    comparison = merged.merge(dc_cols, on="symbol", how="outer")
    comparison["pf_delta"] = comparison["dc_pf"] - comparison["bl_pf"]
    comparison["best_pf"] = comparison[["bl_pf", "dc_pf"]].max(axis=1)
    comparison["best_config"] = np.where(comparison["dc_pf"] > comparison["bl_pf"], "d_chop", "baseline")
    comparison["best_balance"] = comparison[["bl_balance_score", "dc_balance_score"]].max(axis=1)

    # Filter: profitable symbols (PF > 1.0, >= 10 trades on best config)
    profitable = comparison[
        (comparison["best_pf"] > 1.0) &
        (comparison[["bl_trades", "dc_trades"]].max(axis=1) >= 10)
    ].sort_values("best_pf", ascending=False)

    comp_path = output_dir / "symbol_comparison.csv"
    comparison.sort_values("best_pf", ascending=False).to_csv(comp_path, index=False)
    print(f"Comparison CSV: {comp_path}")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 120}")
    print("OOS SUMMARY")
    print("=" * 120)

    total_syms = len(comparison)
    profitable_syms = len(profitable)
    balanced_bl = len(comparison[(comparison["bl_long_pnl"] > 0) & (comparison["bl_short_pnl"] > 0)])
    balanced_dc = len(comparison[(comparison["dc_long_pnl"] > 0) & (comparison["dc_short_pnl"] > 0)])
    dc_wins = len(comparison[comparison["dc_pf"] > comparison["bl_pf"]])
    bl_wins = len(comparison[comparison["bl_pf"] > comparison["dc_pf"]])

    print(f"  Total symbols tested:     {total_syms}")
    print(f"  Profitable (PF>1, >=10t): {profitable_syms}")
    print(f"  Balanced (L+S positive):  baseline={balanced_bl}, d_chop={balanced_dc}")
    print(f"  D_chop wins:              {dc_wins} / {total_syms} ({dc_wins/total_syms*100:.0f}%)")
    print(f"  Baseline wins:            {bl_wins} / {total_syms} ({bl_wins/total_syms*100:.0f}%)")

    # Top 30 by best PF
    print(f"\n{'=' * 120}")
    print("TOP 30 SYMBOLS by OOS PF (best of baseline/d_chop)")
    print("=" * 120)
    print(f"  {'Symbol':18s} | {'BestCfg':8s} | {'PF':>5s} {'Ret':>7s} {'DD':>6s} | {'L$':>6s} {'LPF':>5s} | {'S$':>6s} {'SPF':>5s} | {'Bal':>5s} | {'dPF':>6s}")
    print("  " + "-" * 110)

    top30 = profitable.head(30)
    for _, r in top30.iterrows():
        cfg = r["best_config"]
        pfx = "dc_" if cfg == "d_chop" else "bl_"
        print(
            f"  {r['symbol']:18s} | {cfg:8s} | "
            f"{r[pfx+'pf']:5.3f} {r[pfx+'ret']:+6.1f}% {r[pfx+'dd']:5.1f}% | "
            f"{r[pfx+'long_pnl']:+5.1f} {r[pfx+'long_pf']:5.2f} | "
            f"{r[pfx+'short_pnl']:+5.1f} {r[pfx+'short_pf']:5.02f} | "
            f"{r[pfx+'balance_score']:5.3f} | "
            f"{r['pf_delta']:+.3f}"
        )

    # Top balanced symbols (both sides profitable, sorted by balance score)
    balanced = comparison[
        (comparison["best_pf"] > 1.0) &
        (comparison[["bl_trades", "dc_trades"]].max(axis=1) >= 10)
    ].copy()
    # Check if best config has both sides positive
    best_long = np.where(balanced["best_config"] == "d_chop", balanced["dc_long_pnl"], balanced["bl_long_pnl"])
    best_short = np.where(balanced["best_config"] == "d_chop", balanced["dc_short_pnl"], balanced["bl_short_pnl"])
    balanced["is_balanced"] = (best_long > 0) & (best_short > 0)
    balanced_only = balanced[balanced["is_balanced"]].sort_values("best_balance", ascending=False)

    print(f"\n{'=' * 120}")
    print(f"BALANCED SYMBOLS ({len(balanced_only)} total) -- both L and S profitable on OOS")
    print("=" * 120)
    print(f"  {'Symbol':18s} | {'BestCfg':8s} | {'PF':>5s} {'Ret':>7s} {'DD':>6s} | {'L$':>6s} {'LPF':>5s} | {'S$':>6s} {'SPF':>5s} | {'Bal':>5s}")
    print("  " + "-" * 100)

    for _, r in balanced_only.head(50).iterrows():
        cfg = r["best_config"]
        pfx = "dc_" if cfg == "d_chop" else "bl_"
        print(
            f"  {r['symbol']:18s} | {cfg:8s} | "
            f"{r[pfx+'pf']:5.3f} {r[pfx+'ret']:+6.1f}% {r[pfx+'dd']:5.1f}% | "
            f"{r[pfx+'long_pnl']:+5.1f} {r[pfx+'long_pf']:5.02f} | "
            f"{r[pfx+'short_pnl']:+5.1f} {r[pfx+'short_pf']:5.02f} | "
            f"{r[pfx+'balance_score']:5.3f}"
        )

    # Portfolio candidate list
    portfolio_candidates = balanced_only[balanced_only["best_pf"] >= 1.1].sort_values("best_balance", ascending=False)
    cand_path = output_dir / "portfolio_candidates.csv"
    portfolio_candidates.to_csv(cand_path, index=False)
    print(f"\n  Portfolio candidates (PF>=1.1, balanced): {len(portfolio_candidates)}")
    print(f"  Saved to: {cand_path}")

    if len(portfolio_candidates) > 0:
        cand_symbols = portfolio_candidates["symbol"].tolist()
        print(f"\n  Candidate symbols ({len(cand_symbols)}):")
        for i in range(0, len(cand_symbols), 10):
            batch = cand_symbols[i:i+10]
            print(f"    {', '.join(batch)}")

    print(f"\n{'=' * 120}")
    print(f"Total wall time: {(time.time() - wall_start) / 60:.1f} min")
    print("=" * 120)


if __name__ == "__main__":
    main()
