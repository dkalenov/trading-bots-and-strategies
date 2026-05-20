"""
V4.4 Test: Daily TF Filter + Adaptive Sizing + Sector Limit
Tests improvements that operate on DIFFERENT dimensions than indicators.
"""
from __future__ import annotations
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent.parent))

import csv, time
from pathlib import Path
from datetime import timedelta
import numpy as np
import pandas as pd

try:
    import binance
except ImportError:
    import binance_connector as binance

from momentum_backtest.research_utils import (
    build_is_oos_split, download_market_data, parse_date, run_backtest,
)


def load_ranked_symbols(wide_path, narrow_path, top_n=50):
    rows = []
    with open(wide_path) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    narrow_map = {}
    if Path(narrow_path).exists():
        with open(narrow_path) as f:
            for r in csv.DictReader(f):
                narrow_map[r["symbol"]] = r
    scored = []
    for r in rows:
        pnl, pf, trades = float(r["pnl"]), float(r["profit_factor"]), int(r["trades"])
        avg_r, dd, consec = float(r["avg_r"]), float(r["max_dd_pct"]), int(r["max_consec_losses"])
        if pnl <= 0 or trades < 15:
            continue
        pf_s = min(pf, 3.0)
        trade_s = min(trades / 100, 1.5)
        r_s = min(avg_r, 2.0)
        dd_pen = max(0, (abs(dd) - 10) / 20)
        consec_pen = max(0, (consec - 8) / 10)
        stab = 0
        nr = narrow_map.get(r["symbol"])
        if nr and float(nr["profit_factor"]) >= 1.5 and int(nr["trades"]) >= 30:
            stab = 0.6
        elif nr and float(nr["profit_factor"]) >= 1.3:
            stab = 0.3
        score = pf_s + trade_s * 0.8 + r_s * 0.5 + stab - dd_pen * 0.3 - consec_pen * 0.2
        scored.append((r["symbol"], score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in scored[:top_n]]


def add_daily_filter(symbol_frames_4h, daily_frames):
    """Add daily trend filter columns to 4h frames.
    
    daily_bull: True if daily EMA20 > EMA50 (uptrend on daily)
    daily_bear: True if daily EMA20 < EMA50 (downtrend on daily)
    """
    enhanced = {}
    for symbol, f4h in symbol_frames_4h.items():
        f4h = f4h.copy()
        if symbol in daily_frames:
            daily = daily_frames[symbol].copy()
            # Compute daily EMAs
            daily_close = daily["close"]
            daily_ema20 = daily_close.ewm(span=20, adjust=False).mean()
            daily_ema50 = daily_close.ewm(span=50, adjust=False).mean()
            daily["daily_bull"] = (daily_ema20 > daily_ema50)
            daily["daily_bear"] = (daily_ema20 < daily_ema50)
            
            # Resample to 4h (forward fill daily signals)
            daily_signals = daily[["daily_bull", "daily_bear"]].copy()
            daily_signals.index = pd.DatetimeIndex(daily_signals.index)
            f4h_idx = pd.DatetimeIndex(f4h.index)
            
            # Reindex daily to 4h frequency using forward fill
            combined = daily_signals.reindex(f4h_idx, method="ffill")
            f4h["daily_bull"] = combined["daily_bull"].fillna(False)
            f4h["daily_bear"] = combined["daily_bear"].fillna(False)
        else:
            f4h["daily_bull"] = True   # no filter if no daily data
            f4h["daily_bear"] = True
        enhanced[symbol] = f4h
    return enhanced


COMMON = {
    "signal_mode": "structured", "timeframe": "4h",
    "struct_trend_min": 0.50, "struct_momentum_min": 0.40,
    "struct_timing_min": 0.25, "struct_composite_min": 0.25,
    "struct_weight_profile": "v41",
    "stop_atr": 1.5, "take1_atr": 2.0, "take2_atr": 4.0,
    "partial_exit_pct": 0.1, "trail_after_tp1": False,
    "risk_per_trade": 0.01, "max_positions": 5,
    "max_new_entries_per_bar": 2, "symbol_cooldown_bars": 3,
    "max_entry_gap_atr": 2.0, "funding_mode": "fixed",
    "use_stale_exit": False, "use_regime_exit": False,
    "use_ema_trail": False, "use_time_exit": False,
}

INITIAL_EQUITY = 100.0


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None, help="Start date e.g. 2026-03-01")
    ap.add_argument("--end",   default=None, help="End date e.g. 2026-05-17")
    args = ap.parse_args()

    output_dir = Path("results/v44_test")
    output_dir.mkdir(parents=True, exist_ok=True)

    full_start = parse_date(args.start or "2024-01-01")
    full_end   = parse_date(args.end   or "2026-04-18")

    # If period is shorter than 24 months: run only D_adaptive_wide, no IS/OOS
    period_months = (full_end - full_start).days / 30
    short_mode = period_months < 24

    if short_mode:
        print("=" * 80)
        print("V4.4 RECENT CHECK: D_adaptive_wide only (no IS/OOS split)")
        print(f"Period: {full_start.date()} -> {full_end.date()}")
        print("=" * 80)
    else:
        is_start, is_end, oos_start, oos_end = build_is_oos_split(
            start=full_start, end=full_end, is_months=18, oos_months=99)
        print("=" * 120)
        print("V4.4 TEST: Daily Filter + Adaptive Sizing")
        print(f"IS: {is_start.date()}..{is_end.date()}  |  OOS: {oos_start.date()}..{oos_end.date()}")
        print("=" * 120)

    ranked = load_ranked_symbols(
        "results/per_symbol_wide_4h_20240101_20260418/best_per_symbol.csv",
        "results/per_symbol_scan_4h_20240101_20260418/best_per_symbol.csv",
        top_n=24,
    )

    # Load 4h data
    client = binance.Futures(asynced=False, testnet=False)
    warmup = full_start - timedelta(days=90)

    print("\nLoading 4h data...")
    btc_4h, sym_4h, _ = download_market_data(
        client=client, symbols=["BTCUSDT"] + ranked, interval="4h",
        history_start=warmup, end=full_end, cache_dir=Path("cache/4h"),
        min_history_cutoff=full_start - timedelta(days=365),
        min_bars_in_range=100, range_start=full_start,
        progress_label="4h", log_interval_seconds=30.0,
    )
    selected_4h = {s: sym_4h[s] for s in ranked[:20] if s in sym_4h}
    print(f"  4h: {len(selected_4h)} symbols")

    # Load 1d data for daily filter
    print("Loading 1d data for daily filter...")
    try:
        btc_1d, sym_1d, _ = download_market_data(
            client=client, symbols=["BTCUSDT"] + ranked, interval="1d",
            history_start=warmup - timedelta(days=60), end=full_end,
            cache_dir=Path("cache/1d"),
            min_history_cutoff=full_start - timedelta(days=365),
            min_bars_in_range=50, range_start=full_start,
            progress_label="1d", log_interval_seconds=30.0,
        )
        print(f"  1d: {len(sym_1d)} symbols loaded")
        has_daily = True
    except Exception as e:
        print(f"  ERROR loading daily: {e}")
        has_daily = False

    # Build enhanced frames with daily filter
    if has_daily:
        enhanced_frames = add_daily_filter(selected_4h, sym_1d)
    else:
        enhanced_frames = selected_4h

    # Test configurations
    tests = [
        # A: Baseline (proven v4.1)
        ("A_baseline", selected_4h, {}),
        
        # B: Daily trend filter
        ("B_daily_filter", enhanced_frames, {
            "use_daily_filter": True,
        }),
        
        # C: Adaptive sizing (more $ on high-score signals)
        ("C_adaptive", selected_4h, {
            "use_adaptive_sizing": True,
            "adaptive_min_mult": 0.5,
            "adaptive_max_mult": 1.5,
        }),
        
        # D: Aggressive adaptive (wider range)
        ("D_adaptive_wide", selected_4h, {
            "use_adaptive_sizing": True,
            "adaptive_min_mult": 0.3,
            "adaptive_max_mult": 2.0,
        }),
        
        # E: Daily + Adaptive
        ("E_daily+adaptive", enhanced_frames, {
            "use_daily_filter": True,
            "use_adaptive_sizing": True,
            "adaptive_min_mult": 0.5,
            "adaptive_max_mult": 1.5,
        }),
        
        # F: Daily + Aggressive Adaptive
        ("F_daily+aggr_adpt", enhanced_frames, {
            "use_daily_filter": True,
            "use_adaptive_sizing": True,
            "adaptive_min_mult": 0.3,
            "adaptive_max_mult": 2.0,
        }),
    ]

    results = []

    if short_mode:
        # Short period: run only D_adaptive_wide on full range
        config = dict(COMMON)
        config.update({"use_adaptive_sizing": True, "adaptive_min_mult": 0.3, "adaptive_max_mult": 2.0})
        result = run_backtest(
            symbol_frames=selected_4h, btc_frame=btc_4h,
            start=full_start, end=full_end,
            config_overrides=config, initial_equity=INITIAL_EQUITY,
        )
        s = result.summary
        print(f"\nD_adaptive_wide  {full_start.date()} -> {full_end.date()}")
        print(f"  Trades:        {s.get('closed_trades', 0)}")
        print(f"  Profit Factor: {s.get('profit_factor', 0):.3f}")
        print(f"  Return:        {s.get('total_return_pct', 0):+.1f}%")
        print(f"  Max Drawdown:  {s.get('max_drawdown_pct', 0):.1f}%")
        print(f"  Win Rate:      {s.get('win_rate_pct', 0):.1f}%")
        print(f"  Avg R:         {s.get('avg_r_multiple', 0):.3f}")
        print(f"  TP1 Hit Rate:  {s.get('tp1_hit_rate_pct', 0):.1f}%")
        print(f"\nResults: {output_dir}")
        return

    total = len(tests) * 2
    idx = 0
    started = time.time()

    for name, frames, overrides in tests:
        for period, p_start, p_end in [("IS", is_start, is_end), ("OOS", oos_start, oos_end)]:
            idx += 1
            config = dict(COMMON)
            config.update(overrides)

            result = run_backtest(
                symbol_frames=frames, btc_frame=btc_4h,
                start=p_start, end=p_end,
                config_overrides=config, initial_equity=INITIAL_EQUITY,
            )
            s = result.summary
            row = {
                "test": name, "period": period,
                "trades": s.get("closed_trades", 0),
                "pf": round(s.get("profit_factor", 0) or 0, 3),
                "ret": round(s.get("total_return_pct", 0), 1),
                "wr": round(s.get("win_rate_pct", 0), 1),
                "dd": round(s.get("max_drawdown_pct", 0), 1),
                "avg_r": round(s.get("avg_r_multiple", 0), 3),
            }
            results.append(row)
            elapsed = time.time() - started
            eta = elapsed / idx * (total - idx)
            print(f"  [{idx:2d}/{total}] {name:20s} {period:3s} "
                  f"PF={row['pf']:5.3f} ret={row['ret']:+7.1f}% DD={row['dd']:5.1f}% "
                  f"WR={row['wr']:5.1f}% tr={row['trades']:4d} | ETA {eta/60:.0f}m")

    # Summary
    print(f"\n{'='*120}")
    print("V4.4 RESULTS â€” sorted by OOS PF")
    print("=" * 120)
    print(f"  {'Test':20s} | {'IS PF':>6s} {'IS ret':>7s} {'IS DD':>6s} {'IS tr':>5s} "
          f"| {'OOS PF':>7s} {'OOS ret':>8s} {'OOS DD':>7s} {'OOS WR':>6s} {'OOS tr':>6s} "
          f"| {'vs base':>8s}")
    print("  " + "-" * 115)

    baseline_pf = None
    comparisons = []
    for name, _, _ in tests:
        is_r = next((r for r in results if r["test"] == name and r["period"] == "IS"), None)
        oos_r = next((r for r in results if r["test"] == name and r["period"] == "OOS"), None)
        if is_r and oos_r:
            comparisons.append((name, is_r, oos_r))
            if name == "A_baseline":
                baseline_pf = oos_r["pf"]

    comparisons.sort(key=lambda x: x[2]["pf"], reverse=True)

    for name, is_r, oos_r in comparisons:
        vs = f"{oos_r['pf'] - baseline_pf:+.3f}" if baseline_pf else ""
        mark = " <<<" if oos_r["pf"] == max(c[2]["pf"] for c in comparisons) else ""
        print(f"  {name:20s} | {is_r['pf']:6.3f} {is_r['ret']:+6.1f}% {is_r['dd']:5.1f}% {is_r['trades']:5d} "
              f"| {oos_r['pf']:7.3f} {oos_r['ret']:+7.1f}% {oos_r['dd']:6.1f}% {oos_r['wr']:6.1f} {oos_r['trades']:6d} "
              f"| {vs:>8s}{mark}")

    pd.DataFrame(results).to_csv(output_dir / "v44_results.csv", index=False)
    print(f"\nResults: {output_dir}")


if __name__ == "__main__":
    main()
