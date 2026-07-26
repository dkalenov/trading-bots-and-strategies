"""
Bollinger Bands Strategy — Main entry point.

CLI for running backtests, optimization, and monthly analysis.
"""

import argparse
import json
import sys
from config import Config
from db import Database, BacktestResult
from utils import download_klines_bybit, format_pct, format_currency
from strategy import StrategyParams
from backtester import Backtester


VARIANTS = ['basic', 'rsi_filter', 'squeeze', 'mean_reversion']


def print_stats(stats, variant):
    print(f"\n{'='*60}")
    print(f"  Bollinger Bands Backtest — {variant}")
    print(f"{'='*60}")
    print(f"  Initial Capital:  {format_currency(stats.initial_capital)}")
    print(f"  Final Capital:    {format_currency(stats.final_capital)}")
    print(f"  Total Return:     {format_pct(stats.total_return_pct)}")
    print(f"  Max Drawdown:     {format_pct(-stats.max_drawdown_pct)}")
    print(f"  Sharpe Ratio:     {stats.sharpe_ratio:.3f}")
    print(f"  Sortino Ratio:    {stats.sortino_ratio:.3f}")
    print(f"  Calmar Ratio:     {stats.calmar_ratio:.3f}")
    print(f"  Win Rate:         {stats.win_rate:.1f}%")
    print(f"  Total Trades:     {stats.total_trades}")
    print(f"  Long Trades:      {stats.long_trades} (WR: {stats.long_win_rate:.1f}%)")
    print(f"  Short Trades:     {stats.short_trades} (WR: {stats.short_win_rate:.1f}%)")
    print(f"  Avg Win:          {format_pct(stats.avg_win_pct)}")
    print(f"  Avg Loss:         {format_pct(stats.avg_loss_pct)}")
    print(f"  Profit Factor:    {stats.profit_factor:.2f}")
    print(f"  Expectancy:       {format_currency(stats.expectancy)}")
    print(f"  Avg Duration:     {stats.avg_trade_duration_hours:.1f}h")
    print(f"  Max Consec Wins:  {stats.max_consecutive_wins}")
    print(f"  Max Consec Losses:{stats.max_consecutive_losses}")
    print(f"  Commission:       {format_currency(stats.total_commission)}")
    print(f"  Slippage:         {format_currency(stats.total_slippage)}")
    print(f"  Funding:          {format_currency(stats.total_funding)}")
    print(f"{'='*60}")


def run_backtest(config, symbol, interval, variant, verbose=True):
    params = StrategyParams(**config.strategy_params)
    df = download_klines_bybit(symbol, interval, config.start_date, config.end_date,
                                config.klines_dir, quiet=not verbose)

    if df.empty:
        print(f"No data for {symbol} {interval}")
        return None, None

    if verbose:
        print(f"\nBacktesting {symbol} {interval} — {variant}")
        print(f"Data: {len(df)} bars from {df.index[0]} to {df.index[-1]}")

    backtester = Backtester(
        initial_capital=config.initial_capital,
        risk_pct=config.risk_pct / 100,
        max_leverage=config.leverage,
        commission_rate=config.commission,
        slippage_rate=config.slippage,
        funding_rate=config.funding_rate,
    )

    stats, trades = backtester.run(df, variant, params)

    if verbose:
        print_stats(stats, variant)

    return stats, trades


def run_optimization(config, symbol, interval):
    from itertools import product

    params = StrategyParams(**config.strategy_params)
    df = download_klines_bybit(symbol, interval, config.start_date, config.end_date,
                                config.klines_dir)

    if df.empty:
        print(f"No data for {symbol} {interval}")
        return

    print(f"\nOptimizing {symbol} {interval}")
    print(f"Data: {len(df)} bars")

    # Grid search over bb_timeperiod and bb_nbdevup
    timeperiods = [10, 15, 20, 25, 30]
    nbdevups = [1.5, 2.0, 2.5, 3.0]

    best_sharpe = -999
    best_params = None
    best_stats = None
    results = []

    total = len(timeperiods) * len(nbdevups)
    count = 0

    for tp, nd in product(timeperiods, nbdevups):
        count += 1
        test_params = StrategyParams(
            bb_timeperiod=tp,
            bb_nbdevup=nd,
            bb_nbdevdn=nd,
            rsi_period=params.rsi_period,
            squeeze_atr_period=params.squeeze_atr_period,
            squeeze_threshold=params.squeeze_threshold,
            take_profit_multiplier=params.take_profit_multiplier,
            stop_loss_multiplier=params.stop_loss_multiplier,
            trailing_stop_pct=params.trailing_stop_pct,
        )

        backtester = Backtester(
            initial_capital=config.initial_capital,
            risk_pct=config.risk_pct / 100,
            max_leverage=config.leverage,
            commission_rate=config.commission,
            slippage_rate=config.slippage,
            funding_rate=config.funding_rate,
        )

        stats, trades = backtester.run(df, 'basic', test_params)
        results.append({
            'timeperiod': tp,
            'nbdevup': nd,
            'return_pct': stats.total_return_pct,
            'sharpe': stats.sharpe_ratio,
            'max_dd': stats.max_drawdown_pct,
            'win_rate': stats.win_rate,
            'trades': stats.total_trades,
            'profit_factor': stats.profit_factor,
        })

        if stats.sharpe_ratio > best_sharpe and stats.total_trades >= 10:
            best_sharpe = stats.sharpe_ratio
            best_params = test_params
            best_stats = stats

        if count % 5 == 0:
            print(f"  {count}/{total} done...")

    print(f"\n{'='*60}")
    print(f"  OPTIMIZATION RESULTS — {symbol} {interval}")
    print(f"{'='*60}")

    if best_params:
        print(f"\n  Best Sharpe: {best_sharpe:.3f}")
        print(f"  Best Params: BB({best_params.bb_timeperiod}, {best_params.bb_nbdevup})")
        print_stats(best_stats, 'basic (optimized)')
    else:
        print("  No profitable combination found with >= 10 trades")

    # Save results
    with open(f'opt_results_{symbol}.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n  Saved {len(results)} results to opt_results_{symbol}.json")

    return results


def run_monthly(config, symbol, interval, variant):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    params = StrategyParams(**config.strategy_params)

    start = datetime.strptime(config.start_date, '%Y-%m')
    end = datetime.strptime(config.end_date, '%Y-%m')

    monthly_results = []
    current = start

    while current <= end:
        month_str = current.strftime('%Y-%m')
        next_month = current + relativedelta(months=1)
        next_str = next_month.strftime('%Y-%m')

        df = download_klines_bybit(symbol, interval, month_str, next_str,
                                    config.klines_dir, quiet=True)

        if df.empty or len(df) < 10:
            current = next_month
            continue

        backtester = Backtester(
            initial_capital=config.initial_capital,
            risk_pct=config.risk_pct / 100,
            max_leverage=config.leverage,
            commission_rate=config.commission,
            slippage_rate=config.slippage,
            funding_rate=config.funding_rate,
        )

        stats, trades = backtester.run(df, variant, params)
        monthly_results.append({
            'month': month_str,
            'return_pct': stats.total_return_pct,
            'trades': stats.total_trades,
            'win_rate': stats.win_rate,
            'sharpe': stats.sharpe_ratio,
        })

        current = next_month

    print(f"\n{'='*60}")
    print(f"  MONTHLY BREAKDOWN — {symbol} {interval} — {variant}")
    print(f"{'='*60}")
    print(f"  {'Month':<10} {'Return':>10} {'Trades':>8} {'WR':>8} {'Sharpe':>10}")
    print(f"  {'-'*46}")

    profitable = 0
    for r in monthly_results:
        marker = '*' if r['return_pct'] > 0 else ' '
        print(f"  {r['month']:<10} {format_pct(r['return_pct']):>10} {r['trades']:>8} {r['win_rate']:>7.1f}% {r['sharpe']:>10.3f} {marker}")
        if r['return_pct'] > 0:
            profitable += 1

    print(f"\n  Profitable months: {profitable}/{len(monthly_results)} ({profitable/len(monthly_results)*100:.0f}%)")
    return monthly_results


def save_result_to_db(config, symbol, interval, variant, stats, trades, params):
    db = Database(config.get('database', 'path', 'boll_results.db'))
    result = BacktestResult(
        symbol=symbol,
        interval=interval,
        strategy_name=variant,
        start_date=config.start_date,
        end_date=config.end_date,
        initial_capital=stats.initial_capital,
        final_capital=stats.final_capital,
        total_return_pct=stats.total_return_pct,
        max_drawdown_pct=stats.max_drawdown_pct,
        sharpe_ratio=stats.sharpe_ratio,
        win_rate=stats.win_rate,
        total_trades=stats.total_trades,
        winning_trades=stats.winning_trades,
        losing_trades=stats.losing_trades,
        avg_win_pct=stats.avg_win_pct,
        avg_loss_pct=stats.avg_loss_pct,
        profit_factor=stats.profit_factor,
        avg_trade_duration_hours=stats.avg_trade_duration_hours,
        commission_paid=stats.total_commission,
        params_json=json.dumps({
            'bb_timeperiod': params.bb_timeperiod,
            'bb_nbdevup': params.bb_nbdevup,
            'bb_nbdevdn': params.bb_nbdevdn,
        }),
    )
    row_id = db.save_backtest_result(result)
    db.close()
    print(f"  Saved to DB: row {row_id}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bollinger Bands Strategy Backtester')
    parser.add_argument('--symbol', default=None, help='Trading pair (e.g. BTCUSDT)')
    parser.add_argument('--interval', default=None, help='Kline interval (e.g. 5m, 1h)')
    parser.add_argument('--variant', default='basic', choices=VARIANTS,
                        help='Strategy variant')
    parser.add_argument('--optimize', action='store_true', help='Run parameter optimization')
    parser.add_argument('--monthly', action='store_true', help='Show monthly breakdown')
    parser.add_argument('--save-db', action='store_true', help='Save results to database')
    parser.add_argument('--all-variants', action='store_true', help='Run all variants')

    args = parser.parse_args()
    config = Config()

    symbol = args.symbol or config.symbol
    interval = args.interval or config.interval

    if args.optimize:
        run_optimization(config, symbol, interval)
    elif args.monthly:
        run_monthly(config, symbol, interval, args.variant)
    elif args.all_variants:
        for variant in VARIANTS:
            stats, trades = run_backtest(config, symbol, interval, variant)
            if stats and args.save_db:
                save_result_to_db(config, symbol, interval, variant, stats, trades,
                                  StrategyParams(**config.strategy_params))
    else:
        stats, trades = run_backtest(config, symbol, interval, args.variant)
        if stats and args.save_db:
            save_result_to_db(config, symbol, interval, args.variant, stats, trades,
                              StrategyParams(**config.strategy_params))
