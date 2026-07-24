"""
UTBot Strategy — Realistic Backtester module.

Event-driven backtester with:
- Position sizing: risk_pct of capital per trade
- Funding rate: 0.01% every 8h (futures)
- Commission: 0.04% taker (Binance futures)
- Slippage: dynamic based on order size
- Proper PnL with quantity
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from strategy import UTBotCore, SuperTrendFilter, RSIFilter, Signal, StrategyParams


@dataclass
class Trade:
    entry_time: pd.Timestamp
    exit_time: Optional[pd.Timestamp] = None
    side: str = 'LONG'
    entry_price: float = 0.0
    exit_price: float = 0.0
    quantity: float = 0.0
    notional: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    commission: float = 0.0
    slippage_cost: float = 0.0
    funding_cost: float = 0.0
    exit_reason: str = ''
    stop_loss: float = 0.0
    take_profit: float = 0.0
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0

    @property
    def is_winner(self) -> bool:
        return self.pnl > 0


@dataclass
class BacktestStats:
    initial_capital: float = 0.0
    final_capital: float = 0.0
    total_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_duration_bars: int = 0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    avg_trade_duration_hours: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    total_commission: float = 0.0
    total_slippage: float = 0.0
    total_funding: float = 0.0
    long_trades: int = 0
    short_trades: int = 0
    long_win_rate: float = 0.0
    short_win_rate: float = 0.0


class Backtester:
    def __init__(self, initial_capital: float = 100000,
                 risk_pct: float = 0.01,
                 max_leverage: int = 20,
                 commission_rate: float = 0.0004,
                 slippage_rate: float = 0.0002,
                 funding_rate: float = 0.0001,
                 funding_interval_bars: int = 8,
                 bars_per_year: int = 8760):
        self.initial_capital = initial_capital
        self.risk_pct = risk_pct
        self.max_leverage = max_leverage
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.funding_rate = funding_rate
        self.funding_interval_bars = funding_interval_bars
        self.bars_per_year = bars_per_year

    def run(self, df: pd.DataFrame, strategy_variant: str = 'basic',
            params: Optional[StrategyParams] = None) -> tuple[BacktestStats, list[Trade]]:
        if params is None:
            params = StrategyParams()

        atr_arr = self._calculate_atr(df, params.atr_period)

        match strategy_variant:
            case 'basic':
                return self._run_strategy(df, atr_arr, params)
            case 'supertrend':
                return self._run_supertrend(df, atr_arr, params)
            case 'inner_trend':
                return self._run_inner_trend(df, atr_arr, params)
            case 'take_loss':
                return self._run_take_loss(df, atr_arr, params)
            case _:
                raise ValueError(f"Unknown strategy variant: {strategy_variant}")

    def _calculate_atr(self, df: pd.DataFrame, period: int) -> np.ndarray:
        highs = df['High'].values
        lows = df['Low'].values
        closes = df['Close'].values
        tr = np.maximum(
            highs[1:] - lows[1:],
            np.maximum(
                np.abs(highs[1:] - closes[:-1]),
                np.abs(lows[1:] - closes[:-1])
            )
        )
        atr = np.full(len(closes), np.nan)
        if len(tr) >= period:
            atr[period] = np.mean(tr[:period])
            for i in range(period + 1, len(closes)):
                atr[i] = (atr[i - 1] * (period - 1) + tr[i - 1]) / period
        return atr

    def _open_position(self, entry_price, timestamp, side, sl, tp, atr, capital):
        # Calculate position size based on stop loss distance
        # We want: when price hits SL, loss = risk_pct * capital
        sl_distance = abs(entry_price - sl)
        sl_distance_pct = sl_distance / entry_price

        # Max loss at stop = risk_pct * capital
        max_loss = capital * self.risk_pct

        # Position size: quantity = max_loss / sl_distance
        # But capped by max_leverage
        quantity_by_risk = max_loss / sl_distance if sl_distance > 0 else 0
        notional_by_risk = quantity_by_risk * entry_price
        max_notional = capital * self.max_leverage

        # Take the smaller of risk-based and leverage-capped
        notional = min(notional_by_risk, max_notional)
        quantity = notional / entry_price

        # Entry slippage: assume market order
        slippage_cost = notional * self.slippage_rate

        # Entry commission: taker fee
        commission = notional * self.commission_rate

        return {
            'entry_price': entry_price,
            'entry_time': timestamp,
            'side': side,
            'stop_loss': sl,
            'take_profit': tp,
            'quantity': quantity,
            'notional': notional,
            'entry_commission': commission,
            'entry_slippage': slippage_cost,
            'max_favorable_pct': 0.0,
            'max_adverse_pct': 0.0,
        }

    def _close_position(self, position, exit_price, timestamp, reason, bars_held):
        notional = position['notional']
        quantity = position['quantity']

        # Exit slippage
        exit_slippage = notional * self.slippage_rate

        # Exit commission: taker fee
        exit_commission = notional * self.commission_rate

        # Gross PnL
        if position['side'] == 'LONG':
            gross_pnl = (exit_price - position['entry_price']) * quantity
        else:
            gross_pnl = (position['entry_price'] - exit_price) * quantity

        # Funding cost: charged on borrowed amount (notional - margin)
        funding_periods = bars_held / self.funding_interval_bars
        borrowed = notional * (1 - 1 / self.max_leverage)
        funding_cost = borrowed * self.funding_rate * funding_periods

        # Total costs
        total_commission = position['entry_commission'] + exit_commission
        total_slippage = position['entry_slippage'] + exit_slippage
        total_costs = total_commission + total_slippage + funding_cost

        # Net PnL
        net_pnl = gross_pnl - total_costs

        # PnL percentage (on notional)
        pnl_pct = gross_pnl / notional * 100 if notional > 0 else 0

        duration_hours = (timestamp - position['entry_time']).total_seconds() / 3600

        return Trade(
            entry_time=position['entry_time'],
            exit_time=timestamp,
            side=position['side'],
            entry_price=position['entry_price'],
            exit_price=exit_price,
            quantity=quantity,
            notional=notional,
            pnl=net_pnl,
            pnl_pct=pnl_pct,
            commission=total_commission,
            slippage_cost=total_slippage,
            funding_cost=funding_cost,
            exit_reason=reason,
            stop_loss=position['stop_loss'],
            take_profit=position['take_profit'],
            max_favorable_pct=position['max_favorable_pct'],
            max_adverse_pct=position['max_adverse_pct'],
        )

    def _unrealized_pnl(self, position, current_price):
        if position is None:
            return 0.0
        quantity = position['quantity']
        if position['side'] == 'LONG':
            return (current_price - position['entry_price']) * quantity
        return (position['entry_price'] - current_price) * quantity

    def _run_strategy(self, df, atr_arr, params) -> tuple[BacktestStats, list[Trade]]:
        core = UTBotCore(params)
        capital = self.initial_capital
        position = None
        entry_bar = 0
        trades = []
        equity_curve = [capital]

        for i in range(params.atr_period + 1, len(df)):
            close = df['Close'].iloc[i]
            high = df['High'].iloc[i]
            low = df['Low'].iloc[i]
            atr_val = atr_arr[i]
            timestamp = df.index[i]

            if np.isnan(atr_val):
                equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)
                continue

            signal = core.update(close, atr_val)

            # Check SL/TP on existing position
            if position is not None:
                bars_held = i - entry_bar

                if position['side'] == 'LONG':
                    position['max_favorable_pct'] = max(
                        position['max_favorable_pct'],
                        (high - position['entry_price']) / position['entry_price'] * 100
                    )
                    position['max_adverse_pct'] = max(
                        position['max_adverse_pct'],
                        (position['entry_price'] - low) / position['entry_price'] * 100
                    )
                else:
                    position['max_favorable_pct'] = max(
                        position['max_favorable_pct'],
                        (position['entry_price'] - low) / position['entry_price'] * 100
                    )
                    position['max_adverse_pct'] = max(
                        position['max_adverse_pct'],
                        (high - position['entry_price']) / position['entry_price'] * 100
                    )

                exit_price = None
                exit_reason = ''

                if position['side'] == 'LONG':
                    if low <= position['stop_loss']:
                        exit_price = position['stop_loss']
                        exit_reason = 'STOP_LOSS'
                    elif high >= position['take_profit']:
                        exit_price = position['take_profit']
                        exit_reason = 'TAKE_PROFIT'
                    elif signal == Signal.SELL:
                        exit_price = close
                        exit_reason = 'SIGNAL_SELL'
                else:
                    if high >= position['stop_loss']:
                        exit_price = position['stop_loss']
                        exit_reason = 'STOP_LOSS'
                    elif low <= position['take_profit']:
                        exit_price = position['take_profit']
                        exit_reason = 'TAKE_PROFIT'
                    elif signal == Signal.BUY:
                        exit_price = close
                        exit_reason = 'SIGNAL_BUY'

                if exit_price is not None:
                    trade = self._close_position(position, exit_price, timestamp, exit_reason, bars_held)
                    trades.append(trade)
                    capital += trade.pnl
                    position = None

            # Open new position
            if position is None and capital > 0:
                if signal == Signal.BUY:
                    entry_price = close * (1 + self.slippage_rate)
                    sl = entry_price * (1 - params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 + params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'LONG', sl, tp, atr_val, capital)
                    entry_bar = i
                elif signal == Signal.SELL:
                    entry_price = close * (1 - self.slippage_rate)
                    sl = entry_price * (1 + params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 - params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'SHORT', sl, tp, atr_val, capital)
                    entry_bar = i

            equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)

        # Close any open position at end
        if position is not None:
            bars_held = len(df) - 1 - entry_bar
            exit_price = df['Close'].iloc[-1] * (1 - self.slippage_rate) if position['side'] == 'LONG' else df['Close'].iloc[-1] * (1 + self.slippage_rate)
            trade = self._close_position(position, exit_price, df.index[-1], 'END_OF_DATA', bars_held)
            trades.append(trade)
            capital += trade.pnl

        return self._compute_stats(trades, equity_curve), trades

    def _run_supertrend(self, df, atr_arr, params) -> tuple[BacktestStats, list[Trade]]:
        core = UTBotCore(params)
        st_filter = SuperTrendFilter(params.supertrend_atr_period, params.supertrend_multiplier)
        capital = self.initial_capital
        position = None
        entry_bar = 0
        trades = []
        equity_curve = [capital]

        for i in range(params.atr_period + 1, len(df)):
            close = df['Close'].iloc[i]
            high = df['High'].iloc[i]
            low = df['Low'].iloc[i]
            atr_val = atr_arr[i]
            timestamp = df.index[i]

            if np.isnan(atr_val):
                equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)
                continue

            st_signal = st_filter.update(high, low, close, atr_val)
            signal = core.update(close, atr_val)

            if position is not None:
                bars_held = i - entry_bar

                if position['side'] == 'LONG':
                    position['max_favorable_pct'] = max(position['max_favorable_pct'], (high - position['entry_price']) / position['entry_price'] * 100)
                    position['max_adverse_pct'] = max(position['max_adverse_pct'], (position['entry_price'] - low) / position['entry_price'] * 100)
                else:
                    position['max_favorable_pct'] = max(position['max_favorable_pct'], (position['entry_price'] - low) / position['entry_price'] * 100)
                    position['max_adverse_pct'] = max(position['max_adverse_pct'], (high - position['entry_price']) / position['entry_price'] * 100)

                exit_price = None
                exit_reason = ''

                if position['side'] == 'LONG':
                    if low <= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif high >= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == Signal.SELL:
                        exit_price, exit_reason = close, 'SIGNAL_SELL'
                else:
                    if high >= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif low <= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == Signal.BUY:
                        exit_price, exit_reason = close, 'SIGNAL_BUY'

                if exit_price is not None:
                    trade = self._close_position(position, exit_price, timestamp, exit_reason, bars_held)
                    trades.append(trade)
                    capital += trade.pnl
                    position = None

            if position is None and capital > 0:
                if signal == Signal.BUY and st_signal == 1:
                    entry_price = close * (1 + self.slippage_rate)
                    sl = entry_price * (1 - params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 + params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'LONG', sl, tp, atr_val, capital)
                    entry_bar = i
                elif signal == Signal.SELL and st_signal == -1:
                    entry_price = close * (1 - self.slippage_rate)
                    sl = entry_price * (1 + params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 - params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'SHORT', sl, tp, atr_val, capital)
                    entry_bar = i

            equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)

        if position is not None:
            bars_held = len(df) - 1 - entry_bar
            exit_price = df['Close'].iloc[-1] * (1 - self.slippage_rate) if position['side'] == 'LONG' else df['Close'].iloc[-1] * (1 + self.slippage_rate)
            trade = self._close_position(position, exit_price, df.index[-1], 'END_OF_DATA', bars_held)
            trades.append(trade)
            capital += trade.pnl

        return self._compute_stats(trades, equity_curve), trades

    def _run_inner_trend(self, df, atr_arr, params) -> tuple[BacktestStats, list[Trade]]:
        core = UTBotCore(params)
        rsi_filter = RSIFilter(params.rsi_period)
        capital = self.initial_capital
        position = None
        entry_bar = 0
        trades = []
        equity_curve = [capital]

        for i in range(params.atr_period + 1, len(df)):
            close = df['Close'].iloc[i]
            high = df['High'].iloc[i]
            low = df['Low'].iloc[i]
            atr_val = atr_arr[i]
            timestamp = df.index[i]

            if np.isnan(atr_val):
                equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)
                continue

            rsi = rsi_filter.update(close)
            signal = core.update(close, atr_val)

            if position is not None:
                bars_held = i - entry_bar

                if position['side'] == 'LONG':
                    position['max_favorable_pct'] = max(position['max_favorable_pct'], (high - position['entry_price']) / position['entry_price'] * 100)
                    position['max_adverse_pct'] = max(position['max_adverse_pct'], (position['entry_price'] - low) / position['entry_price'] * 100)
                else:
                    position['max_favorable_pct'] = max(position['max_favorable_pct'], (position['entry_price'] - low) / position['entry_price'] * 100)
                    position['max_adverse_pct'] = max(position['max_adverse_pct'], (high - position['entry_price']) / position['entry_price'] * 100)

                exit_price = None
                exit_reason = ''

                if position['side'] == 'LONG':
                    if low <= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif high >= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == Signal.SELL:
                        exit_price, exit_reason = close, 'SIGNAL_SELL'
                else:
                    if high >= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif low <= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == Signal.BUY:
                        exit_price, exit_reason = close, 'SIGNAL_BUY'

                if exit_price is not None:
                    trade = self._close_position(position, exit_price, timestamp, exit_reason, bars_held)
                    trades.append(trade)
                    capital += trade.pnl
                    position = None

            if position is None and capital > 0:
                if signal == Signal.BUY and rsi > 55:
                    entry_price = close * (1 + self.slippage_rate)
                    sl = entry_price * (1 - params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 + params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'LONG', sl, tp, atr_val, capital)
                    entry_bar = i
                elif signal == Signal.SELL and rsi < 45:
                    entry_price = close * (1 - self.slippage_rate)
                    sl = entry_price * (1 + params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 - params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'SHORT', sl, tp, atr_val, capital)
                    entry_bar = i

            equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)

        if position is not None:
            bars_held = len(df) - 1 - entry_bar
            exit_price = df['Close'].iloc[-1] * (1 - self.slippage_rate) if position['side'] == 'LONG' else df['Close'].iloc[-1] * (1 + self.slippage_rate)
            trade = self._close_position(position, exit_price, df.index[-1], 'END_OF_DATA', bars_held)
            trades.append(trade)
            capital += trade.pnl

        return self._compute_stats(trades, equity_curve), trades

    def _run_take_loss(self, df, atr_arr, params) -> tuple[BacktestStats, list[Trade]]:
        core = UTBotCore(params)
        capital = self.initial_capital
        position = None
        entry_bar = 0
        trades = []
        equity_curve = [capital]
        peak_price = 0.0

        for i in range(params.atr_period + 1, len(df)):
            close = df['Close'].iloc[i]
            high = df['High'].iloc[i]
            low = df['Low'].iloc[i]
            atr_val = atr_arr[i]
            timestamp = df.index[i]

            if np.isnan(atr_val):
                equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)
                continue

            signal = core.update(close, atr_val)

            if position is not None:
                bars_held = i - entry_bar

                if position['side'] == 'LONG':
                    position['max_favorable_pct'] = max(position['max_favorable_pct'], (high - position['entry_price']) / position['entry_price'] * 100)
                    position['max_adverse_pct'] = max(position['max_adverse_pct'], (position['entry_price'] - low) / position['entry_price'] * 100)
                    peak_price = max(peak_price, high)
                else:
                    position['max_favorable_pct'] = max(position['max_favorable_pct'], (position['entry_price'] - low) / position['entry_price'] * 100)
                    position['max_adverse_pct'] = max(position['max_adverse_pct'], (high - position['entry_price']) / position['entry_price'] * 100)
                    peak_price = min(peak_price, low) if peak_price > 0 else low

                exit_price = None
                exit_reason = ''

                if position['side'] == 'LONG':
                    if low <= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif high >= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == Signal.SELL:
                        exit_price, exit_reason = close, 'SIGNAL_SELL'
                    else:
                        trailing_stop_price = peak_price * (1 - params.trailing_stop_pct / 100)
                        if low < trailing_stop_price and peak_price > position['entry_price']:
                            exit_price, exit_reason = trailing_stop_price, 'TRAILING_STOP'
                else:
                    if high >= position['stop_loss']:
                        exit_price, exit_reason = position['stop_loss'], 'STOP_LOSS'
                    elif low <= position['take_profit']:
                        exit_price, exit_reason = position['take_profit'], 'TAKE_PROFIT'
                    elif signal == Signal.BUY:
                        exit_price, exit_reason = close, 'SIGNAL_BUY'
                    else:
                        trailing_stop_price = peak_price * (1 + params.trailing_stop_pct / 100)
                        if high > trailing_stop_price and peak_price < position['entry_price']:
                            exit_price, exit_reason = trailing_stop_price, 'TRAILING_STOP'

                if exit_price is not None:
                    trade = self._close_position(position, exit_price, timestamp, exit_reason, bars_held)
                    trades.append(trade)
                    capital += trade.pnl
                    position = None
                    peak_price = 0.0

            if position is None and capital > 0:
                if signal == Signal.BUY:
                    entry_price = close * (1 + self.slippage_rate)
                    sl = entry_price * (1 - params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 + params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'LONG', sl, tp, atr_val, capital)
                    entry_bar = i
                    peak_price = high
                elif signal == Signal.SELL:
                    entry_price = close * (1 - self.slippage_rate)
                    sl = entry_price * (1 + params.stop_loss_multiplier * atr_val / entry_price)
                    tp = entry_price * (1 - params.take_profit_multiplier * atr_val / entry_price)
                    position = self._open_position(entry_price, timestamp, 'SHORT', sl, tp, atr_val, capital)
                    entry_bar = i
                    peak_price = low

            equity_curve.append(capital + self._unrealized_pnl(position, close) if position else capital)

        if position is not None:
            bars_held = len(df) - 1 - entry_bar
            exit_price = df['Close'].iloc[-1] * (1 - self.slippage_rate) if position['side'] == 'LONG' else df['Close'].iloc[-1] * (1 + self.slippage_rate)
            trade = self._close_position(position, exit_price, df.index[-1], 'END_OF_DATA', bars_held)
            trades.append(trade)
            capital += trade.pnl

        return self._compute_stats(trades, equity_curve), trades

    def _compute_stats(self, trades: list[Trade], equity_curve: list[float]) -> BacktestStats:
        stats = BacktestStats()
        stats.initial_capital = self.initial_capital
        stats.total_trades = len(trades)

        if not trades:
            stats.final_capital = self.initial_capital
            return stats

        stats.final_capital = equity_curve[-1] if equity_curve else self.initial_capital
        stats.total_return_pct = (stats.final_capital - self.initial_capital) / self.initial_capital * 100

        # Drawdown
        peak = equity_curve[0]
        max_dd = 0.0
        max_dd_bars = 0
        current_dd_bars = 0
        for eq in equity_curve:
            if eq > peak:
                peak = eq
                current_dd_bars = 0
            else:
                current_dd_bars += 1
            dd = (peak - eq) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
                max_dd_bars = current_dd_bars
        stats.max_drawdown_pct = max_dd
        stats.max_drawdown_duration_bars = max_dd_bars

        # Sharpe ratio
        returns = np.diff(equity_curve) / np.array(equity_curve[:-1]) if len(equity_curve) > 1 else []
        returns = [r for r in returns if not np.isnan(r) and np.isfinite(r)]
        if len(returns) > 1 and np.std(returns) > 0:
            ann = np.sqrt(self.bars_per_year)
            stats.sharpe_ratio = np.mean(returns) / np.std(returns) * ann
            neg_returns = [r for r in returns if r < 0]
            if neg_returns and np.std(neg_returns) > 0:
                stats.sortino_ratio = np.mean(returns) / np.std(neg_returns) * ann
        if stats.max_drawdown_pct > 0:
            stats.calmar_ratio = stats.total_return_pct / stats.max_drawdown_pct

        # Win/loss
        winners = [t for t in trades if t.is_winner]
        losers = [t for t in trades if not t.is_winner]
        stats.winning_trades = len(winners)
        stats.losing_trades = len(losers)
        stats.win_rate = len(winners) / len(trades) * 100 if trades else 0

        # Side stats
        long_trades = [t for t in trades if t.side == 'LONG']
        short_trades = [t for t in trades if t.side == 'SHORT']
        stats.long_trades = len(long_trades)
        stats.short_trades = len(short_trades)
        long_winners = [t for t in long_trades if t.is_winner]
        short_winners = [t for t in short_trades if t.is_winner]
        stats.long_win_rate = len(long_winners) / len(long_trades) * 100 if long_trades else 0
        stats.short_win_rate = len(short_winners) / len(short_trades) * 100 if short_trades else 0

        # Averages
        stats.avg_win_pct = np.mean([t.pnl_pct for t in winners]) if winners else 0.0
        stats.avg_loss_pct = np.mean([t.pnl_pct for t in losers]) if losers else 0.0

        total_win = sum(t.pnl for t in winners) if winners else 0
        total_loss = abs(sum(t.pnl for t in losers)) if losers else 0
        stats.profit_factor = total_win / total_loss if total_loss > 0 else float('inf')

        stats.expectancy = np.mean([t.pnl for t in trades])

        # Duration
        durations = []
        for t in trades:
            if t.exit_time and t.entry_time:
                durations.append((t.exit_time - t.entry_time).total_seconds() / 3600)
        stats.avg_trade_duration_hours = np.mean(durations) if durations else 0

        # Consecutive wins/losses
        max_consec_wins = 0
        max_consec_losses = 0
        current_consec = 0
        for t in trades:
            if t.is_winner:
                current_consec = current_consec + 1 if current_consec > 0 else 1
                max_consec_wins = max(max_consec_wins, current_consec)
            else:
                current_consec = current_consec - 1 if current_consec < 0 else -1
                max_consec_losses = max(max_consec_losses, abs(current_consec))
        stats.max_consecutive_wins = max_consec_wins
        stats.max_consecutive_losses = max_consec_losses

        stats.total_commission = sum(t.commission for t in trades)
        stats.total_slippage = sum(t.slippage_cost for t in trades)
        stats.total_funding = sum(t.funding_cost for t in trades)

        return stats
