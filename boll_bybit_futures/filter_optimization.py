"""
Bollinger Bands Strategy — Filter Optimization module.

Tests various filter combinations on batch backtest results
to find optimal symbol selection criteria.
"""

import json
import argparse
import numpy as np
from pathlib import Path


def load_batch_results(path: str = 'batch_results.json') -> list[dict]:
    with open(path, 'r') as f:
        data = json.load(f)
    # Support both formats: list or dict with 'results' key
    if isinstance(data, list):
        return data
    return data.get('results', [])


def apply_filter(symbols: list[dict], conditions: dict) -> list[dict]:
    result = []
    for s in symbols:
        if 'sharpe_min' in conditions and s.get('sharpe', 0) < conditions['sharpe_min']:
            continue
        if 'wr_min' in conditions and s.get('win_rate', 0) < conditions['wr_min']:
            continue
        if 'dd_max' in conditions and s.get('max_drawdown', 0) > conditions['dd_max']:
            continue
        if 'trades_min' in conditions and s.get('trades', 0) < conditions['trades_min']:
            continue
        if 'return_min' in conditions and s.get('return_pct', 0) < conditions['return_min']:
            continue
        if 'pf_min' in conditions and s.get('profit_factor', 0) < conditions['pf_min']:
            continue
        result.append(s)
    return result


def evaluate_filter(symbols: list[dict], filtered: list[dict]) -> dict:
    if not filtered:
        return {
            'total': 0, 'profitable': 0, 'profitable_pct': 0,
            'avg_return': 0, 'avg_sharpe': 0, 'avg_wr': 0,
            'avg_dd': 0, 'median_return': 0, 'worst_return': 0,
            'best_return': 0, 'avg_trades': 0, 'avg_pf': 0,
            'selectivity': 0, 'score': 0,
        }

    returns = [s['return_pct'] for s in filtered]
    profitable = sum(1 for r in returns if r > 0)

    prof_pct = profitable / len(filtered) * 100
    avg_ret = np.mean(returns)
    avg_sharpe = np.mean([s['sharpe'] for s in filtered])
    avg_wr = np.mean([s['win_rate'] for s in filtered])
    avg_dd = np.mean([s['max_drawdown'] for s in filtered])
    selectivity = len(filtered) / len(symbols) * 100

    score = (
        prof_pct * 0.3 +
        min(avg_ret, 100) * 0.25 +
        avg_sharpe * 20 * 0.2 +
        (100 - avg_dd) * 0.15 +
        min(selectivity, 50) * 0.1
    )

    return {
        'total': len(filtered),
        'profitable': profitable,
        'profitable_pct': round(prof_pct, 1),
        'avg_return': round(avg_ret, 2),
        'avg_sharpe': round(avg_sharpe, 3),
        'avg_wr': round(avg_wr, 1),
        'avg_dd': round(avg_dd, 2),
        'median_return': round(np.median(returns), 2),
        'worst_return': round(min(returns), 2),
        'best_return': round(max(returns), 2),
        'avg_trades': round(np.mean([s['trades'] for s in filtered]), 1),
        'avg_pf': round(np.mean([s['profit_factor'] for s in filtered]), 2),
        'selectivity': round(selectivity, 1),
        'score': round(score, 2),
    }


def grid_search_filters(symbols: list[dict], verbose: bool = True) -> list[dict]:
    sharpe_mins = [0, 0.5, 1.0, 1.5, 2.0]
    wr_mins = [0, 40, 45, 50, 55]
    dd_maxs = [100, 30, 20, 15, 10, 8, 5]
    trades_mins = [0, 5, 10, 20, 30]
    pf_mins = [0, 1.0, 1.5, 2.0]

    results = []

    for sharpe_min in sharpe_mins:
        for wr_min in wr_mins:
            for dd_max in dd_maxs:
                for trades_min in trades_mins:
                    for pf_min in pf_mins:
                        conditions = {
                            'sharpe_min': sharpe_min,
                            'wr_min': wr_min,
                            'dd_max': dd_max,
                            'trades_min': trades_min,
                            'pf_min': pf_min,
                        }
                        filtered = apply_filter(symbols, conditions)
                        if len(filtered) < 3:
                            continue

                        metrics = evaluate_filter(symbols, filtered)
                        metrics['conditions'] = conditions
                        results.append(metrics)

    results.sort(key=lambda x: -x['score'])
    return results


def optimize_filters(symbols: list[dict], verbose: bool = True) -> dict:
    if verbose:
        print(f"\nFilter Optimization: {len(symbols)} symbols")
        print(f"Profitable: {sum(1 for s in symbols if s['return_pct'] > 0)}/{len(symbols)}")

    grid = grid_search_filters(symbols, verbose=False)
    if verbose and grid:
        print(f"\n{'='*90}")
        print(f"  TOP 10 FILTER RESULTS")
        print(f"{'='*90}")
        print(f"  {'#':>3} {'Conditions':<50} {'N':>4} {'Prof%':>6} {'AvgRet':>9} {'Sharpe':>8} {'DD':>7} {'Score':>7}")
        print(f"  {'─'*88}")
        for i, r in enumerate(grid[:10]):
            cond = r['conditions']
            cond_str = (f"Sharpe>={cond.get('sharpe_min',0)} WR>={cond.get('wr_min',0)} "
                       f"DD<={cond.get('dd_max',100)} Trades>={cond.get('trades_min',0)} "
                       f"PF>={cond.get('pf_min',0)}")
            print(f"  {i+1:>3} {cond_str:<50} {r['total']:>4} {r['profitable_pct']:>5.0f}% "
                  f"{r['avg_return']:>+8.2f}% {r['avg_sharpe']:>8.3f} "
                  f"{r['avg_dd']:>6.1f}% {r['score']:>7.2f}")

    best = grid[0] if grid else None

    if verbose and best:
        print(f"\n  BEST FILTER:")
        print(f"    {best['total']} symbols, {best['profitable_pct']:.0f}% profitable, "
              f"avg return {best['avg_return']:+.2f}%, Sharpe {best['avg_sharpe']:.3f}")

    return {
        'total_symbols': len(symbols),
        'grid_top10': grid[:10] if grid else [],
        'best_filter': best,
    }


def main():
    parser = argparse.ArgumentParser(description='Bollinger Bands Filter Optimization')
    parser.add_argument('--input', default='batch_results.json')
    parser.add_argument('--output', default=None)

    args = parser.parse_args()

    symbols = load_batch_results(args.input)
    results = optimize_filters(symbols)

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to {args.output}")


if __name__ == '__main__':
    main()
