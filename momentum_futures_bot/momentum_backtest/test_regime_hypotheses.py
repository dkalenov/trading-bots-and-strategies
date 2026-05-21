"""
Test Regime Hypotheses — IS/OOS validation
============================================
Tests whether adding BTC regime awareness improves OOS performance.

Hypotheses tested:
  A) Baseline (current D_adaptive_wide)
  B) BTC Choppy Filter — raise signal thresholds when BTC ADX is low
  C) Tighter BTC Volatility Filter — narrower ATR band
  D) Reduced Exposure — fewer max positions
  E) Lower Risk — smaller risk_per_trade
  F) Combined — best elements from above

Expected outcome: Baseline wins on OOS (regime filters overfit).
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent.parent))

import time
from datetime import timedelta
from pathlib import Path

import pandas as pd

try:
    import binance
except ImportError:
    import binance_connector as binance  # type: ignore

from momentum_backtest.research_utils import (
    build_is_oos_split, download_market_data, parse_date, run_backtest,
)


# Top-20 validated symbols
SYMBOLS = [
    "BRUSDT", "HBARUSDT", "DOGEUSDT", "XVGUSDT", "1000PEPEUSDT",
    "RSRUSDT", "WIFUSDT", "ARUSDT", "STGUSDT", "DYMUSDT",
    "1000FLOKIUSDT", "ONEUSDT", "JASMYUSDT", "RONINUSDT", "LQTYUSDT",
    "WLDUSDT", "METISUSDT", "PORTALUSDT", "AVAXUSDT", "ATAUSDT",
]

# Base production config (D_adaptive_wide)
BASE = {
    "signal_mode": "structured", "timeframe": "4h",
    "struct_weight_profile": "v41",
    "struct_trend_min": 0.50, "struct_momentum_min": 0.40,
    "struct_timing_min": 0.25, "struct_composite_min": 0.25,
    "stop_atr": 1.5, "take1_atr": 2.0, "take2_atr": 4.0,
    "partial_exit_pct": 0.1, "risk_per_trade": 0.01,
    "max_positions": 5, "max_new_entries_per_bar": 2,
    "symbol_cooldown_bars": 3, "max_entry_gap_atr": 2.0,
    "trail_after_tp1": False,
    "use_adaptive_sizing": True,
    "adaptive_min_mult": 0.5, "adaptive_max_mult": 1.5,
    "use_daily_filter": False, "use_stale_exit": False,
    "use_regime_exit": False, "use_ema_trail": False,
    "use_time_exit": False, "funding_mode": "fixed",
    # Choppy defaults (inactive at mult=1.0)
    "btc_choppy_threshold_mult": 1.0, "btc_choppy_adx": 20.0,
    # ATR filter defaults
    "btc_atr_low_filter": 0.5, "btc_atr_high_filter": 2.0,
}

EQUITY = 100.0


def make_config(name, **overrides):
    """Create a named test config from BASE + overrides."""
    cfg = dict(BASE)
    cfg.update(overrides)
    return (name, cfg)


# ──────────────────────────────────────────────────────────────────────────────
# Define all test hypotheses
# ──────────────────────────────────────────────────────────────────────────────
TESTS = [
    # A: Baseline — no changes
    make_config("A_baseline"),

    # === HYPOTHESIS 1: BTC Choppy Filter (raise thresholds when ADX low) ===
    # B1: Mild choppy filter (ADX<20, thresholds +20%)
    make_config("B1_choppy_adx20_x1.2",
                btc_choppy_adx=20.0, btc_choppy_threshold_mult=1.2),
    # B2: Moderate choppy filter (ADX<20, thresholds +50%)
    make_config("B2_choppy_adx20_x1.5",
                btc_choppy_adx=20.0, btc_choppy_threshold_mult=1.5),
    # B3: Aggressive choppy filter (ADX<25, thresholds +30%)
    make_config("B3_choppy_adx25_x1.3",
                btc_choppy_adx=25.0, btc_choppy_threshold_mult=1.3),
    # B4: Wide choppy zone (ADX<30, thresholds +20%)
    make_config("B4_choppy_adx30_x1.2",
                btc_choppy_adx=30.0, btc_choppy_threshold_mult=1.2),
    # B5: Extreme filter (ADX<25, thresholds +80%)
    make_config("B5_choppy_adx25_x1.8",
                btc_choppy_adx=25.0, btc_choppy_threshold_mult=1.8),

    # === HYPOTHESIS 2: Tighter BTC Volatility Band ===
    # C1: Don't trade when BTC ATR too high (crisis)
    make_config("C1_atr_high1.5",
                btc_atr_high_filter=1.5),
    # C2: Tighter both ends
    make_config("C2_atr_0.6_1.8",
                btc_atr_low_filter=0.6, btc_atr_high_filter=1.8),

    # === HYPOTHESIS 3: Reduced Exposure ===
    # D1: Max 3 positions (less portfolio risk)
    make_config("D1_max3pos",
                max_positions=3),
    # D2: Max 4 positions
    make_config("D2_max4pos",
                max_positions=4),
    # D3: Max 3 + only 1 entry per bar
    make_config("D3_max3_1entry",
                max_positions=3, max_new_entries_per_bar=1),

    # === HYPOTHESIS 4: Lower Risk Per Trade ===
    # E1: risk 0.7% instead of 1%
    make_config("E1_risk0.7pct",
                risk_per_trade=0.007),
    # E2: risk 0.5%
    make_config("E2_risk0.5pct",
                risk_per_trade=0.005),

    # === HYPOTHESIS 5: Combined (Choppy + Reduced) ===
    # F1: Mild choppy + 4 positions
    make_config("F1_choppy1.2_max4",
                btc_choppy_adx=20.0, btc_choppy_threshold_mult=1.2,
                max_positions=4),
    # F2: Choppy + lower risk
    make_config("F2_choppy1.3_risk0.7",
                btc_choppy_adx=25.0, btc_choppy_threshold_mult=1.3,
                risk_per_trade=0.007),
    # F3: All-in conservative
    make_config("F3_conservative",
                btc_choppy_adx=25.0, btc_choppy_threshold_mult=1.3,
                max_positions=4, risk_per_trade=0.007,
                btc_atr_high_filter=1.8),

    # === HYPOTHESIS 6: Higher Min Entry Score ===
    make_config("G1_min_entry0.3",
                struct_min_entry_score=0.3),
    make_config("G2_min_entry0.35",
                struct_min_entry_score=0.35),
]


def main():
    full_start = parse_date("2024-01-01")
    full_end = parse_date("2026-04-18")

    is_start, is_end, oos_start, oos_end = build_is_oos_split(
        start=full_start, end=full_end, is_months=18, oos_months=99,
    )

    print("=" * 120)
    print("REGIME HYPOTHESIS TEST")
    print(f"IS: {is_start.date()} -> {is_end.date()}  |  OOS: {oos_start.date()} -> {oos_end.date()}")
    print(f"Tests: {len(TESTS)} configs x 2 periods = {len(TESTS) * 2} backtests")
    print("=" * 120)

    # Load data
    client = binance.Futures(asynced=False, testnet=False)
    warmup = full_start - timedelta(days=90)

    print("\nLoading 4h data...")
    btc_4h, sym_4h, skipped = download_market_data(
        client=client,
        symbols=["BTCUSDT"] + SYMBOLS,
        interval="4h",
        history_start=warmup,
        end=full_end,
        cache_dir=Path("cache/4h"),
        min_history_cutoff=full_start - timedelta(days=365),
        min_bars_in_range=100,
        range_start=full_start,
        progress_label="4h",
        log_interval_seconds=30.0,
    )
    selected = {s: sym_4h[s] for s in SYMBOLS if s in sym_4h}
    print(f"  Loaded: {len(selected)} symbols\n")

    # Run all tests
    output_dir = Path("results/regime_hypotheses")
    output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    total = len(TESTS) * 2
    idx = 0
    t0 = time.time()

    for name, config in TESTS:
        for period, p_start, p_end in [("IS", is_start, is_end), ("OOS", oos_start, oos_end)]:
            idx += 1
            result = run_backtest(
                symbol_frames=selected,
                btc_frame=btc_4h,
                start=p_start,
                end=p_end,
                config_overrides=config,
                initial_equity=EQUITY,
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
                "tp1": round(s.get("tp1_hit_rate_pct", 0), 1),
            }
            results.append(row)
            elapsed = time.time() - t0
            eta = elapsed / idx * (total - idx)
            print(f"  [{idx:2d}/{total}] {name:25s} {period:3s} "
                  f"PF={row['pf']:5.3f} ret={row['ret']:+7.1f}% "
                  f"DD={row['dd']:5.1f}% WR={row['wr']:5.1f}% "
                  f"tr={row['trades']:4d} | ETA {eta/60:.0f}m")

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 120}")
    print("REGIME HYPOTHESIS RESULTS -- sorted by OOS PF")
    print("=" * 120)

    header = (f"  {'Test':25s} | {'IS PF':>6s} {'IS ret':>7s} {'IS DD':>6s} {'IS tr':>5s} "
              f"| {'OOS PF':>7s} {'OOS ret':>8s} {'OOS DD':>7s} {'OOS WR':>6s} {'OOS tr':>6s} "
              f"| {'vs base':>8s}")
    print(header)
    print("  " + "-" * 115)

    baseline_oos_pf = None
    comparisons = []
    for name, _ in TESTS:
        is_r = next((r for r in results if r["test"] == name and r["period"] == "IS"), None)
        oos_r = next((r for r in results if r["test"] == name and r["period"] == "OOS"), None)
        if is_r and oos_r:
            comparisons.append((name, is_r, oos_r))
            if name == "A_baseline":
                baseline_oos_pf = oos_r["pf"]

    comparisons.sort(key=lambda x: x[2]["pf"], reverse=True)

    best_oos_pf = max(c[2]["pf"] for c in comparisons) if comparisons else 0

    for name, is_r, oos_r in comparisons:
        vs = f"{oos_r['pf'] - baseline_oos_pf:+.3f}" if baseline_oos_pf else ""
        mark = " <<<" if oos_r["pf"] == best_oos_pf else ""
        is_overfit = " OVERFIT" if is_r["pf"] > baseline_oos_pf and oos_r["pf"] < baseline_oos_pf else ""
        print(f"  {name:25s} | {is_r['pf']:6.3f} {is_r['ret']:+6.1f}% "
              f"{is_r['dd']:5.1f}% {is_r['trades']:5d} "
              f"| {oos_r['pf']:7.3f} {oos_r['ret']:+7.1f}% "
              f"{oos_r['dd']:6.1f}% {oos_r['wr']:6.1f} {oos_r['trades']:6d} "
              f"| {vs:>8s}{mark}{is_overfit}")

    # Save results
    df = pd.DataFrame(results)
    df.to_csv(output_dir / "regime_results.csv", index=False)

    # ── Verdict ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 120}")
    print("VERDICT")
    print("=" * 120)

    baseline_row = next((c for c in comparisons if c[0] == "A_baseline"), None)
    best_row = comparisons[0] if comparisons else None

    if baseline_row and best_row:
        b_oos = baseline_row[2]
        w_oos = best_row[2]

        if best_row[0] == "A_baseline":
            print("  >> BASELINE WINS. Regime filters do NOT improve OOS performance.")
            print("     Adding complexity = adding overfitting risk. Keep strategy as-is.")
        elif w_oos["pf"] > b_oos["pf"] + 0.05:
            print(f"  >> {best_row[0]} beats baseline by +{w_oos['pf'] - b_oos['pf']:.3f} PF on OOS.")
            print(f"     OOS PF: {w_oos['pf']:.3f} vs baseline {b_oos['pf']:.3f}")
            print(f"     But check: IS PF was {best_row[1]['pf']:.3f} -> is improvement real or overfit?")
            # IS-OOS degradation check
            degrad_best = best_row[1]["pf"] - w_oos["pf"]
            degrad_base = baseline_row[1]["pf"] - b_oos["pf"]
            print(f"     IS->OOS degradation: baseline={degrad_base:.3f}, winner={degrad_best:.3f}")
            if degrad_best > degrad_base + 0.05:
                print("     WARNING: Winner degrades MORE than baseline = possible overfit")
            else:
                print("     OK: Winner degrades similarly to baseline = may be real improvement")
        else:
            print(f"  >> Marginal improvement: {best_row[0]} +{w_oos['pf'] - b_oos['pf']:.3f} PF")
            print("     Difference too small to be statistically significant.")
            print("     Recommendation: keep baseline to minimize complexity risk.")

    print(f"\n  Results: {output_dir}")
    print()


if __name__ == "__main__":
    main()
