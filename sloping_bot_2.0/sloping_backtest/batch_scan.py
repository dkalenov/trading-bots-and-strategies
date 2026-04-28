"""Batch test: extended coin scan."""
import subprocess
import sys
import json
from pathlib import Path

SYMBOLS = [
    # Meme / high-vol (DOGE worked → test similar)
    "1000PEPEUSDT",
    "1000SHIBUSDT",
    "FLOKIUSDT",
    "BONKUSDT",
    "NOTUSDT",
    # Mid-cap momentum
    "NEARUSDT",
    "INJUSDT",
    "FETUSDT",
    "TIAUSDT",
    "RENDERUSDT",
    # L1/L2 trending
    "APTUSDT",
    "SEIUSDT",
    "OPUSDT",
    "ARBUSDT",
    "MATICUSDT",
]

INTERVAL = "1h"
EXEC_INTERVAL = "1m"
TRIALS = 500
BASE_DIR = Path("sloping_backtest/results/batch_v3_ext")
BASE_DIR.mkdir(parents=True, exist_ok=True)

results = []
for symbol in SYMBOLS:
    output_dir = BASE_DIR / f"{symbol.lower()}_{INTERVAL}"
    print(f"\n{'='*60}")
    print(f"  {symbol} @ {INTERVAL}")
    print(f"{'='*60}")
    
    cmd = [
        sys.executable, "-m", "sloping_backtest", "optimize",
        "--symbol", symbol,
        "--interval", INTERVAL,
        "--execution-interval", EXEC_INTERVAL,
        "--start", "2024-01",
        "--end", "2025-12",
        "--search-space", "sloping_backtest/search_space.stage1.json",
        "--trials", str(TRIALS),
        "--seed", "42",
        "--objective", "return_over_max_drawdown",
        "--validation-fraction", "0.2",
        "--min-trades", "10",
        "--slippage-bps", "1",
        "--leverage", "20",
        "--intrabar-mode", "conservative",
        "--output-dir", str(output_dir),
    ]
    
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        print(proc.stdout)
        if proc.returncode != 0:
            print(f"  ERROR: {proc.stderr[-300:]}")
            continue
            
        best_file = output_dir / "optimization_best.json"
        if best_file.exists():
            data = json.loads(best_file.read_text())
            val = data.get("best_validation_summary") or data.get("best_train_summary", {})
            params = data.get("best_params", {})
            results.append({
                "symbol": symbol,
                "trades": val.get("trades", 0),
                "return_pct": val.get("return_pct", 0),
                "max_dd": val.get("max_drawdown_pct", 0),
                "pf": val.get("profit_factor", 0),
                "win_rate": val.get("win_rate_pct", 0),
                "sharpe": val.get("sharpe", 0),
                "trail": params.get("trail_after_tp1", False),
                "vol_f": params.get("vol_filter", False),
            })
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT")
    except Exception as e:
        print(f"  ERROR: {e}")

print(f"\n\n{'='*90}")
print(f"  EXTENDED SCAN RESULTS (validation 5 mo)")
print(f"{'='*90}")
print(f"{'Symbol':<16} {'Trades':>6} {'Return%':>9} {'MaxDD%':>8} {'PF':>6} {'Win%':>6} {'Sharpe':>7} {'Trail':>6} {'VolF':>5}")
print("-" * 80)
for r in sorted(results, key=lambda x: x["return_pct"], reverse=True):
    trail = "Yes" if r["trail"] else "No"
    vol_f = "Yes" if r["vol_f"] else "No"
    print(f"{r['symbol']:<16} {r['trades']:>6} {r['return_pct']:>9.2f} {r['max_dd']:>8.2f} {r['pf']:>6.2f} {r['win_rate']:>6.1f} {r['sharpe']:>7.2f} {trail:>6} {vol_f:>5}")
