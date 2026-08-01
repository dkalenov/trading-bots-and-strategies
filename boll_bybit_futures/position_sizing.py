"""
Bollinger Bands Strategy — Position Sizing Optimization module.

Tests different risk percentages per trade to find optimal position sizing.
"""

import json
import argparse
import pandas as pd
import numpy as np

from config import Config
from strategy import StrategyParams
from backtester import Backtester
from utils import download_klines_bybit


def interval_to_bars_per_year(interval: str) -> int:
    mapping = {
        '1m': 525600, '3m': 175200, '5m': 105120, '15m': 35040, '30m': 17520,
        '1h': 8760, '2h': 4380, '4h': 2190, '6h': 1460, '8h': 1095, '12h': 730,
        '1d': 365, '3d': 122, '1w': 52, '1M': 12,
    }
    return mapping.get(interval, 8760)


def test_risk_levels(df: pd.DataFrame, strategy_variant: str, params: StrategyParams,
                     risk_levels: list[float], bars_per_year: int,
                     initial_capital: float = 100000) -> list[dict]:
    results = []

    for risk_pct in risk_levels:
        bt = Backtester(
            initial_capital=initial_capital,
            risk_pct=risk_pct,
            commission_rate=0.0004,
            slippage_rate=0.0002,
            funding_rate=0.0001,
            funding_interval_bars=8,
            max_leverage=20,
            bars_per_year=bars_per_year,
        )

        stats, trades = bt.run(df, strategy_variant, params)

        result = {
            'risk_pct': risk_pct,
            'risk_label': f"{risk_pct*100:.1f}%",
            'return_pct': round(stats.total_return_pct, 2),
            'sharpe': round(stats.sharpe_ratio, 3),
            'sortino': round(stats.sortino_ratio, 3),
            'max_drawdown': round(stats.max_drawdown_pct, 2),
            'calmar': round(stats.calmar_ratio, 3),
            'win_rate': round(stats.win_rate, 1),
            'trades': stats.total_trades,
            'profit_factor': round(stats.profit_factor, 2),
            'expectancy': round(stats.expectancy, 2),
            'avg_win_pct': round(stats.avg_win_pct, 2),
            'avg_loss_pct': round(stats.avg_loss_pct, 2),
            'final_capital': round(stats.final_capital, 2),
            'total_commission': round(stats.total_commission, 2),
            'total_slippage': round(stats.total_slippage, 2),
            'total_funding': round(stats.total_funding, 2),
            'max_consec_wins': stats.max_consecutive_wins,
            'max_consec_losses': stats.max_consecutive_losses,
        }
        results.append(result)

    return results


def optimize_position_sizing(symbol: str, interval: str, start_date: str, end_date: str,
                             strategy_variant: str = 'basic',
                             params: StrategyParams = None,
                             risk_levels: list[float] = None,
                             klines_dir: str = 'klines',
                             verbose: bool = True) -> dict:
    if params is None:
        params = StrategyParams(bb_timeperiod=20, bb_nbdevup=2.0, bb_nbdevdn=2.0)

    if risk_levels is None:
        risk_levels = [0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05]

    bars_per_year = interval_to_bars_per_year(interval)

    if verbose:
        print(f"\nPosition Sizing Optimization: {symbol} {interval}")
        print(f"Strategy: {strategy_variant}")
        print(f"Params: BB({params.bb_timeperiod}, {params.bb_nbdevup})")
        print(f"Risk levels: {[f'{r*100:.1f}%' for r in risk_levels]}")

    df = download_klines_bybit(symbol, interval, start_date, end_date, klines_dir)
    if verbose:
        print(f"Data: {len(df)} candles")

    results = test_risk_levels(df, strategy_variant, params, risk_levels, bars_per_year)

    best_sharpe = max(results, key=lambda x: x['sharpe'])
    best_calmar = max(results, key=lambda x: x['calmar'])
    best_return = max(results, key=lambda x: x['return_pct'])

    if verbose:
        print(f"\n{'='*80}")
        print(f"  POSITION SIZING RESULTS — {symbol}")
        print(f"{'='*80}")
        print(f"  {'Risk':>6} {'Return':>10} {'Sharpe':>8} {'Sortino':>8} {'MaxDD':>8} "
              f"{'Calmar':>8} {'WR':>6} {'PF':>6} {'Expct$':>10}")
        print(f"  {'─'*78}")
        for r in results:
            marker = ""
            if r['risk_pct'] == best_sharpe['risk_pct']:
                marker = " <-- best Sharpe"
            elif r['risk_pct'] == best_calmar['risk_pct']:
                marker = " <-- best Calmar"
            print(f"  {r['risk_label']:>6} {r['return_pct']:>+9.2f}% {r['sharpe']:>8.3f} "
                  f"{r['sortino']:>8.3f} {r['max_drawdown']:>7.2f}% {r['calmar']:>8.3f} "
                  f"{r['win_rate']:>5.0f}% {r['profit_factor']:>6.2f} "
                  f"${r['expectancy']:>+9.2f}{marker}")

        print(f"\n  Optimal by Sharpe:     {best_sharpe['risk_label']} "
              f"(Sharpe={best_sharpe['sharpe']:.3f}, Return={best_sharpe['return_pct']:+.2f}%, "
              f"MaxDD={best_sharpe['max_drawdown']:.2f}%)")
        print(f"  Optimal by Calmar:     {best_calmar['risk_label']} "
              f"(Calmar={best_calmar['calmar']:.3f}, Return={best_calmar['return_pct']:+.2f}%, "
              f"MaxDD={best_calmar['max_drawdown']:.2f}%)")
        print(f"  Optimal by Return:     {best_return['risk_label']} "
              f"(Return={best_return['return_pct']:+.2f}%, Sharpe={best_return['sharpe']:.3f}, "
              f"MaxDD={best_return['max_drawdown']:.2f}%)")
        print(f"{'='*80}")

    return {
        'symbol': symbol,
        'interval': interval,
        'strategy': strategy_variant,
        'params': {
            'bb_timeperiod': params.bb_timeperiod,
            'bb_nbdevup': params.bb_nbdevup,
            'bb_nbdevdn': params.bb_nbdevdn,
        },
        'results': results,
        'best_sharpe': best_sharpe,
        'best_calmar': best_calmar,
        'best_return': best_return,
    }


def main():
    parser = argparse.ArgumentParser(description='Bollinger Bands Position Sizing Optimization')
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--interval', default='5m')
    parser.add_argument('--start', default='2024-01')
    parser.add_argument('--end', default='2025-06')
    parser.add_argument('--strategy', default='basic',
                        choices=['basic', 'rsi_filter', 'squeeze', 'mean_reversion'])
    parser.add_argument('--bb-timeperiod', type=int, default=20)
    parser.add_argument('--bb-nbdevup', type=float, default=2.0)
    parser.add_argument('--output', default=None)

    args = parser.parse_args()

    params = StrategyParams(
        bb_timeperiod=args.bb_timeperiod,
        bb_nbdevup=args.bb_nbdevup,
        bb_nbdevdn=args.bb_nbdevup,
    )

    results = optimize_position_sizing(
        args.symbol, args.interval, args.start, args.end,
        strategy_variant=args.strategy, params=params,
    )

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to {args.output}")


if __name__ == '__main__':
    main()
