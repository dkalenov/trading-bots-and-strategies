from __future__ import annotations

import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sloping_bot.sloping import Sloping

from .data import ensure_ohlcv_frame


EPSILON = 1e-12


@dataclass(frozen=True)
class StrategyParams:
    length: int = 50
    atr_length: int = 14
    min_space: int = 3
    min_touches: int = 2
    breakout_buffer: float = 0.1
    slope_filter: bool = True
    use_trend_filter: bool = True
    trend_sma: int = 200
    vol_filter: bool = False
    vol_lookback: int = 5
    take1: float = 1.0
    take2: float = 2.5
    stop: float = 1.0
    portion: float = 0.5
    risk_pct: float = 0.01
    trail_after_tp1: bool = False
    trail_atr: float = 1.5


@dataclass(frozen=True)
class ExecutionParams:
    initial_cash: float = 1000.0
    commission: float = 0.0005
    slippage_bps: float = 0.0
    leverage: float = 20.0
    enforce_leverage_cap: bool = True
    close_open_position_at_end: bool = True
    intrabar_mode: Literal["conservative", "optimistic"] = "conservative"
    allow_long: bool = True
    allow_short: bool = True


@dataclass
class PositionState:
    trade_id: int
    symbol: str
    side_long: bool
    entry_index: int
    entry_time: pd.Timestamp
    entry_price: float
    initial_quantity: float
    remaining_quantity: float
    atr_at_entry: float
    line_slope: float
    line_intercept: float
    take1_price: float | None
    take2_price: float | None
    stop_price: float
    initial_stop_price: float
    portion: float
    take1_triggered: bool = False
    partial_exit_done: bool = False
    breakeven_stop_price: float | None = None
    partial_quantity: float = 0.0
    partial_fill_price: float | None = None
    partial_fill_time: pd.Timestamp | None = None
    gross_pnl: float = 0.0
    commission_paid: float = 0.0
    trail_high: float | None = None  # highest high (long) / lowest low (short) since TP1


@dataclass
class BacktestResult:
    summary: dict
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    monthly_returns: pd.DataFrame


def _validate_strategy_params(strategy: StrategyParams) -> None:
    if not 1 <= strategy.length <= 500:
        raise ValueError("length must be in [1, 500]")
    if not 1 <= strategy.atr_length <= 500:
        raise ValueError("atr_length must be in [1, 500]")
    if not 1 <= strategy.trend_sma <= 500:
        raise ValueError("trend_sma must be in [1, 500]")
    if strategy.stop <= 0:
        raise ValueError("stop must be positive")
    if strategy.take1 <= 0:
        raise ValueError("take1 must be positive")
    if strategy.take2 < 0:
        raise ValueError("take2 must be non-negative")
    if strategy.take2 and strategy.take2 < strategy.take1:
        raise ValueError("take2 must be >= take1 when enabled")
    if not 0 < strategy.portion <= 1:
        raise ValueError("portion must be in (0, 1]")
    if not 0 < strategy.risk_pct <= 1:
        raise ValueError("risk_pct must be in (0, 1]")
    if strategy.min_touches < 1:
        raise ValueError("min_touches must be >= 1")
    if strategy.min_space < 0:
        raise ValueError("min_space must be >= 0")
    if strategy.breakout_buffer < 0:
        raise ValueError("breakout_buffer must be >= 0")


def _to_timestamp(value: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp


def _infer_bar_delta(index: pd.Index) -> pd.Timedelta:
    timestamps = pd.DatetimeIndex(pd.to_datetime(index))
    if len(timestamps) < 2:
        return pd.Timedelta(minutes=1)
    deltas = timestamps.to_series().diff().dropna()
    deltas = deltas[deltas > pd.Timedelta(0)]
    if deltas.empty:
        return pd.Timedelta(minutes=1)
    return deltas.median()


def _market_fill_price(price: float, action: Literal["buy", "sell"], slippage_bps: float) -> float:
    slippage_multiplier = slippage_bps / 10_000.0
    if action == "buy":
        return price * (1.0 + slippage_multiplier)
    return price * (1.0 - slippage_multiplier)


def _limit_fill_price(price: float) -> float:
    return price


def _trade_direction(side_long: bool) -> int:
    return 1 if side_long else -1


def _realized_pnl(side_long: bool, quantity: float, entry_price: float, exit_price: float) -> float:
    direction = _trade_direction(side_long)
    return (exit_price - entry_price) * quantity * direction


def _unrealized_pnl(position: PositionState, mark_price: float) -> float:
    return _realized_pnl(position.side_long, position.remaining_quantity, position.entry_price, mark_price)


def _should_trade(timestamp: pd.Timestamp, trade_start: pd.Timestamp | None) -> bool:
    return trade_start is None or timestamp >= trade_start


def _compute_position_size(
    cash: float,
    entry_price: float,
    atr: float,
    strategy: StrategyParams,
    execution: ExecutionParams,
) -> float:
    if cash <= 0 or entry_price <= 0 or atr <= 0 or strategy.stop <= 0:
        return 0.0

    risk_amount = cash * strategy.risk_pct
    stop_distance = atr * strategy.stop
    quantity = risk_amount / stop_distance

    if execution.enforce_leverage_cap and execution.leverage > 0:
        max_notional = cash * execution.leverage
        quantity = min(quantity, max_notional / entry_price)

    return max(quantity, 0.0)


def _average_exit_price(position: PositionState, final_exit_price: float) -> float:
    final_qty = max(position.initial_quantity - position.partial_quantity, 0.0)
    if position.partial_quantity <= EPSILON:
        return final_exit_price
    total_notional = 0.0
    if position.partial_fill_price is not None:
        total_notional += position.partial_fill_price * position.partial_quantity
    total_notional += final_exit_price * final_qty
    if position.initial_quantity <= EPSILON:
        return final_exit_price
    return total_notional / position.initial_quantity


def _build_trade_record(
    position: PositionState,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    exit_index: int,
) -> dict:
    net_pnl = position.gross_pnl - position.commission_paid
    duration_hours = (exit_time - position.entry_time).total_seconds() / 3600.0
    entry_notional = position.entry_price * position.initial_quantity
    avg_exit_price = _average_exit_price(position, exit_price)

    return {
        "trade_id": position.trade_id,
        "symbol": position.symbol,
        "side": "long" if position.side_long else "short",
        "entry_time": position.entry_time,
        "exit_time": exit_time,
        "entry_price": position.entry_price,
        "final_exit_price": exit_price,
        "avg_exit_price": avg_exit_price,
        "quantity": position.initial_quantity,
        "partial_quantity": position.partial_quantity,
        "remaining_exit_quantity": max(position.initial_quantity - position.partial_quantity, 0.0),
        "atr_at_entry": position.atr_at_entry,
        "line_slope": position.line_slope,
        "line_intercept": position.line_intercept,
        "take1_price": position.take1_price,
        "take2_price": position.take2_price,
        "stop_price": position.initial_stop_price,
        "breakeven_stop_price": position.breakeven_stop_price,
        "take1_triggered": position.take1_triggered,
        "partial_exit_done": position.partial_exit_done,
        "partial_fill_time": position.partial_fill_time,
        "partial_fill_price": position.partial_fill_price,
        "bars_held": max(exit_index - position.entry_index, 0),
        "duration_hours": duration_hours,
        "entry_notional": entry_notional,
        "gross_pnl": position.gross_pnl,
        "commission_paid": position.commission_paid,
        "net_pnl": net_pnl,
        "return_on_notional_pct": (net_pnl / entry_notional * 100.0) if entry_notional else 0.0,
        "exit_reason": exit_reason,
    }


def _close_position(
    position: PositionState,
    cash: float,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    execution: ExecutionParams,
    exit_index: int,
) -> tuple[None, float, dict]:
    fee = abs(exit_price * position.remaining_quantity) * execution.commission
    gross = _realized_pnl(position.side_long, position.remaining_quantity, position.entry_price, exit_price)
    position.gross_pnl += gross
    position.commission_paid += fee
    cash += gross - fee
    record = _build_trade_record(position, exit_time, exit_price, exit_reason, exit_index)
    return None, cash, record


def _process_exchange_orders(
    position: PositionState,
    high: float,
    low: float,
    timestamp: pd.Timestamp,
    cash: float,
    execution: ExecutionParams,
    index: int,
) -> tuple[PositionState | None, float, dict | None]:
    stop_hit = low <= position.stop_price if position.side_long else high >= position.stop_price
    take_hit = False
    if position.take2_price is not None:
        take_hit = high >= position.take2_price if position.side_long else low <= position.take2_price

    if not stop_hit and not take_hit:
        return position, cash, None

    if stop_hit and take_hit:
        take_first = execution.intrabar_mode == "optimistic"
    else:
        take_first = take_hit

    if take_first:
        exit_price = _limit_fill_price(position.take2_price)  # type: ignore[arg-type]
        exit_reason = "take2"
    else:
        exit_price = _market_fill_price(
            position.stop_price,
            "sell" if position.side_long else "buy",
            execution.slippage_bps,
        )
        exit_reason = "breakeven" if position.breakeven_stop_price is not None else "stop"

    return _close_position(position, cash, timestamp, exit_price, exit_reason, execution, index)


def _process_take1(
    position: PositionState,
    high: float,
    low: float,
    timestamp: pd.Timestamp,
    cash: float,
    execution: ExecutionParams,
    index: int,
) -> tuple[PositionState | None, float, dict | None]:
    if position.partial_exit_done or position.take1_price is None:
        return position, cash, None

    # Live бот мониторит TP1 через bookTicker WS в реальном времени (не по Close),
    # поэтому для OHLC-бэктеста проверяем по High/Low — если цена дошла интрабар, TP1 сработал.
    take1_hit = high >= position.take1_price if position.side_long else low <= position.take1_price
    if not take1_hit:
        return position, cash, None

    partial_quantity = min(position.initial_quantity * position.portion, position.remaining_quantity)
    if partial_quantity <= EPSILON:
        return position, cash, None

    # Fill price = take1_price (MARKET ордер при достижении уровня, fill ~= уровень TP1)
    fill_price = _market_fill_price(
        position.take1_price,
        "sell" if position.side_long else "buy",
        execution.slippage_bps,
    )
    fee = abs(fill_price * partial_quantity) * execution.commission
    gross = _realized_pnl(position.side_long, partial_quantity, position.entry_price, fill_price)

    cash += gross - fee
    position.gross_pnl += gross
    position.commission_paid += fee
    position.remaining_quantity -= partial_quantity
    position.partial_quantity = partial_quantity
    position.partial_fill_price = fill_price
    position.partial_fill_time = timestamp
    position.take1_triggered = True
    position.partial_exit_done = True

    if position.remaining_quantity <= EPSILON:
        position.remaining_quantity = 0.0
        return _close_position(position, cash, timestamp, fill_price, "take1_market", execution, index)

    position.breakeven_stop_price = (
        position.entry_price * (0.999 if position.side_long else 1.001)
    )
    position.stop_price = position.breakeven_stop_price
    # Initialize trailing stop tracking
    position.trail_high = high if position.side_long else low
    return position, cash, None


def _update_trailing_stop(
    position: PositionState,
    high: float,
    low: float,
    strategy: StrategyParams,
) -> None:
    """Update trailing stop after TP1 has triggered."""
    if not strategy.trail_after_tp1 or not position.partial_exit_done:
        return
    if position.trail_high is None:
        return

    trail_distance = position.atr_at_entry * strategy.trail_atr

    if position.side_long:
        position.trail_high = max(position.trail_high, high)
        new_stop = position.trail_high - trail_distance
        if new_stop > position.stop_price:
            position.stop_price = new_stop
    else:
        position.trail_high = min(position.trail_high, low)
        new_stop = position.trail_high + trail_distance
        if new_stop < position.stop_price:
            position.stop_price = new_stop


def _process_execution_bar(
    position: PositionState,
    high: float,
    low: float,
    timestamp: pd.Timestamp,
    cash: float,
    execution: ExecutionParams,
    strategy: StrategyParams,
    index: int,
) -> tuple[PositionState | None, float, dict | None]:
    # Update trailing stop before checking orders
    _update_trailing_stop(position, high, low, strategy)

    if position.partial_exit_done:
        return _process_exchange_orders(position, high, low, timestamp, cash, execution, index)

    take1_hit = False
    if position.take1_price is not None:
        take1_hit = high >= position.take1_price if position.side_long else low <= position.take1_price

    stop_hit = low <= position.stop_price if position.side_long else high >= position.stop_price

    if not take1_hit:
        return _process_exchange_orders(position, high, low, timestamp, cash, execution, index)

    if stop_hit and execution.intrabar_mode == "conservative":
        return _process_exchange_orders(position, high, low, timestamp, cash, execution, index)

    position, cash, closed_trade = _process_take1(
        position,
        high,
        low,
        timestamp,
        cash,
        execution,
        index,
    )
    if closed_trade or position is None:
        return position, cash, closed_trade

    # IMPORTANT: after TP1, do NOT check STOP/TP2 on the same bar!
    # In live, the breakeven/trailing stop is placed after partial close
    # and starts working from the next tick.
    return position, cash, None


def _build_monthly_returns(equity_curve: pd.DataFrame, initial_cash: float) -> pd.DataFrame:
    if equity_curve.empty:
        return pd.DataFrame(columns=["month", "equity", "return_pct"])

    equity_series = equity_curve.set_index("time")["equity"].copy()
    monthly_equity = equity_series.resample("ME").last()

    rows: list[dict] = []
    previous_equity = initial_cash
    for month_end, equity in monthly_equity.items():
        return_pct = ((equity - previous_equity) / previous_equity * 100.0) if previous_equity else 0.0
        rows.append(
            {
                "month": month_end.strftime("%Y-%m"),
                "equity": float(equity),
                "return_pct": float(return_pct),
            }
        )
        previous_equity = float(equity)
    return pd.DataFrame(rows)


def _infer_periods_per_year(index: pd.Series) -> float | None:
    if len(index) < 2:
        return None
    timestamps = pd.DatetimeIndex(pd.to_datetime(index))
    deltas = timestamps.to_series().diff().dropna().dt.total_seconds()
    if deltas.empty:
        return None
    median_seconds = deltas.median()
    if not median_seconds or median_seconds <= 0:
        return None
    return (365.25 * 24 * 3600) / median_seconds


def _calculate_sharpe(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0
    equity_series = equity_curve.set_index("time")["equity"]
    returns = equity_series.pct_change().dropna()
    if returns.empty or returns.std(ddof=0) <= EPSILON:
        return 0.0
    periods_per_year = _infer_periods_per_year(equity_curve["time"])
    if not periods_per_year:
        return 0.0
    return float((returns.mean() / returns.std(ddof=0)) * math.sqrt(periods_per_year))


def _build_summary(
    strategy: StrategyParams,
    execution: ExecutionParams,
    symbol: str,
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
    monthly_returns: pd.DataFrame,
) -> dict:
    final_equity = float(equity_curve["equity"].iloc[-1]) if not equity_curve.empty else execution.initial_cash
    net_profit = final_equity - execution.initial_cash
    return_pct = (net_profit / execution.initial_cash * 100.0) if execution.initial_cash else 0.0

    if equity_curve.empty:
        max_drawdown_pct = 0.0
    else:
        equity_values = equity_curve["equity"].to_numpy(dtype=float)
        peaks = np.maximum.accumulate(equity_values)
        drawdowns = np.where(peaks > 0, (equity_values - peaks) / peaks * 100.0, 0.0)
        max_drawdown_pct = float(abs(drawdowns.min()))

    if trades.empty:
        win_rate_pct = 0.0
        profit_factor = 0.0
        avg_trade_net = 0.0
        avg_duration_hours = 0.0
        long_trades = 0
        short_trades = 0
    else:
        wins = trades.loc[trades["net_pnl"] > 0, "net_pnl"]
        losses = trades.loc[trades["net_pnl"] < 0, "net_pnl"]
        win_rate_pct = float((len(wins) / len(trades)) * 100.0)
        gross_profit = float(wins.sum()) if not wins.empty else 0.0
        gross_loss = float(abs(losses.sum())) if not losses.empty else 0.0
        profit_factor = float(gross_profit / gross_loss) if gross_loss > EPSILON else float("inf" if gross_profit > 0 else 0.0)
        avg_trade_net = float(trades["net_pnl"].mean())
        avg_duration_hours = float(trades["duration_hours"].mean())
        long_trades = int((trades["side"] == "long").sum())
        short_trades = int((trades["side"] == "short").sum())

    profitable_months = int((monthly_returns["return_pct"] > 0).sum()) if not monthly_returns.empty else 0
    total_months = len(monthly_returns)
    profit_month_ratio_pct = (profitable_months / total_months * 100.0) if total_months else 0.0

    return {
        "symbol": symbol,
        "initial_cash": execution.initial_cash,
        "final_equity": final_equity,
        "net_profit": net_profit,
        "return_pct": return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "return_over_max_drawdown": return_pct / max(max_drawdown_pct, 0.1),
        "sharpe": _calculate_sharpe(equity_curve),
        "trades": int(len(trades)),
        "win_rate_pct": win_rate_pct,
        "profit_factor": profit_factor,
        "avg_trade_net": avg_trade_net,
        "avg_duration_hours": avg_duration_hours,
        "long_trades": long_trades,
        "short_trades": short_trades,
        "profitable_months": profitable_months,
        "loss_months": total_months - profitable_months,
        "profit_month_ratio_pct": profit_month_ratio_pct,
        **asdict(strategy),
    }


def run_backtest(
    data: pd.DataFrame,
    strategy: StrategyParams,
    execution: ExecutionParams | None = None,
    *,
    symbol: str = "UNKNOWN",
    trade_start: str | pd.Timestamp | None = None,
    execution_data: pd.DataFrame | None = None,
) -> BacktestResult:
    execution = execution or ExecutionParams()
    _validate_strategy_params(strategy)
    df = ensure_ohlcv_frame(data)
    if df.index.has_duplicates:
        df = df.loc[~df.index.duplicated(keep="last")].copy()
    trade_start_ts = _to_timestamp(trade_start)
    signal_delta = _infer_bar_delta(df.index)
    signal_index = pd.DatetimeIndex(df.index)
    signal_interval_ends = signal_index[1:].append(pd.DatetimeIndex([signal_index[-1] + signal_delta]))
    open_values = df["Open"].to_numpy(dtype=float, copy=False)
    high_values = df["High"].to_numpy(dtype=float, copy=False)
    low_values = df["Low"].to_numpy(dtype=float, copy=False)
    close_values = df["Close"].to_numpy(dtype=float, copy=False)

    exec_df: pd.DataFrame | None = None
    exec_index: pd.DatetimeIndex | None = None
    exec_left_bounds: np.ndarray | None = None
    exec_right_bounds: np.ndarray | None = None
    exec_high_values: np.ndarray | None = None
    exec_low_values: np.ndarray | None = None
    if execution_data is not None:
        exec_df = ensure_ohlcv_frame(execution_data)
        if exec_df.index.has_duplicates:
            exec_df = exec_df.loc[~exec_df.index.duplicated(keep="last")].copy()
        exec_delta = _infer_bar_delta(exec_df.index)
        if exec_delta > signal_delta:
            raise ValueError("execution_data interval must be less than or equal to the signal timeframe")
        exec_start = pd.Timestamp(df.index[0])
        exec_end = pd.Timestamp(df.index[-1]) + signal_delta
        if pd.Timestamp(exec_df.index[0]) < exec_start or pd.Timestamp(exec_df.index[-1]) >= exec_end:
            exec_df = exec_df.loc[(exec_df.index >= exec_start) & (exec_df.index < exec_end)].copy()
        if exec_df.empty:
            raise ValueError("execution_data has no overlap with the signal timeframe range")
        exec_index = pd.DatetimeIndex(exec_df.index)
        exec_left_bounds = exec_index.searchsorted(signal_index, side="left")
        exec_right_bounds = exec_index.searchsorted(signal_interval_ends, side="left")
        exec_high_values = exec_df["High"].to_numpy(dtype=float, copy=False)
        exec_low_values = exec_df["Low"].to_numpy(dtype=float, copy=False)

    indicator = Sloping(
        length=strategy.length,
        atr_length=strategy.atr_length,
        min_space=strategy.min_space,
        debug=False,
        min_touches=strategy.min_touches,
        breakout_buffer=strategy.breakout_buffer,
        slope_filter=strategy.slope_filter,
        use_trend_filter=strategy.use_trend_filter,
        trend_sma=strategy.trend_sma,
        vol_filter=strategy.vol_filter,
        vol_lookback=strategy.vol_lookback,
    )

    cash = float(execution.initial_cash)
    position: PositionState | None = None
    trades: list[dict] = []
    equity_points: list[dict] = []
    next_trade_id = 1
    for index in range(len(df)):
        timestamp = pd.Timestamp(signal_index[index])
        interval_end = pd.Timestamp(signal_interval_ends[index])
        open_price = float(open_values[index])
        high_price = float(high_values[index])
        low_price = float(low_values[index])
        close_price = float(close_values[index])

        indicator.add_kline(
            int(timestamp.timestamp() * 1000),
            open_price,
            high_price,
            low_price,
            close_price,
        )

        # --- Порядок обработки важен! ---
        # В live: TP1 проверяется по Close на закрытии свечи, а STOP/TP2 — resting orders
        # на бирже (срабатывают внутри свечи). Если в одной свече цена задела TP1 и потом STOP,
        # в live TP1 закроет partial + переведёт стоп в breakeven ДО того, как STOP исполнится.
        # Поэтому сначала TP1, потом exchange orders.

        if position is not None:
            if exec_index is not None and exec_left_bounds is not None and exec_right_bounds is not None and exec_high_values is not None and exec_low_values is not None:
                left = int(exec_left_bounds[index])
                right = int(exec_right_bounds[index])

                if left >= right:
                    position, cash, closed_trade = _process_execution_bar(
                        position,
                        high_price,
                        low_price,
                        interval_end,
                        cash,
                        execution,
                        strategy,
                        index,
                    )
                    if closed_trade:
                        trades.append(closed_trade)
                else:
                    for exec_pos in range(left, right):
                        if position is None:
                            break
                        position, cash, closed_trade = _process_execution_bar(
                            position,
                            float(exec_high_values[exec_pos]),
                            float(exec_low_values[exec_pos]),
                            pd.Timestamp(exec_index[exec_pos]),
                            cash,
                            execution,
                            strategy,
                            index,
                        )
                        if closed_trade:
                            trades.append(closed_trade)
            else:
                position, cash, closed_trade = _process_execution_bar(
                    position,
                    high_price,
                    low_price,
                    interval_end,
                    cash,
                    execution,
                    strategy,
                    index,
                )
                if closed_trade:
                    trades.append(closed_trade)

        if position is None and index < len(signal_index) - 1 and _should_trade(timestamp, trade_start_ts):
            signal = indicator.get_value(
                support=execution.allow_short,
                resistance=execution.allow_long,
            )
            if signal:
                entry_price = _market_fill_price(
                    close_price,
                    "buy" if signal.side else "sell",
                    execution.slippage_bps,
                )
                quantity = _compute_position_size(cash, entry_price, signal.atr, strategy, execution)
                if quantity > EPSILON:
                    entry_fee = abs(entry_price * quantity) * execution.commission
                    cash -= entry_fee
                    position = PositionState(
                        trade_id=next_trade_id,
                        symbol=symbol,
                        side_long=bool(signal.side),
                        entry_index=index,
                        entry_time=interval_end,
                        entry_price=float(entry_price),
                        initial_quantity=float(quantity),
                        remaining_quantity=float(quantity),
                        atr_at_entry=float(signal.atr),
                        line_slope=float(signal.line[0]),
                        line_intercept=float(signal.line[1]),
                        take1_price=(
                            entry_price + signal.atr * strategy.take1 if signal.side
                            else entry_price - signal.atr * strategy.take1
                        ),
                        take2_price=(
                            entry_price + signal.atr * strategy.take2 if signal.side else entry_price - signal.atr * strategy.take2
                        ) if strategy.take2 > 0 else None,
                        stop_price=(
                            entry_price - signal.atr * strategy.stop if signal.side
                            else entry_price + signal.atr * strategy.stop
                        ),
                        initial_stop_price=(
                            entry_price - signal.atr * strategy.stop if signal.side
                            else entry_price + signal.atr * strategy.stop
                        ),
                        portion=strategy.portion,
                        commission_paid=entry_fee,
                    )
                    next_trade_id += 1

        equity = cash
        if position is not None:
            equity += _unrealized_pnl(position, close_price)
        equity_points.append(
            {
                "time": interval_end,
                "cash": cash,
                "equity": equity,
                "position_open": position is not None,
            }
        )

    if position is not None and execution.close_open_position_at_end:
        last_timestamp = pd.Timestamp(df.index[-1]) + signal_delta
        last_close = float(df["Close"].iloc[-1])
        exit_price = _market_fill_price(
            last_close,
            "sell" if position.side_long else "buy",
            execution.slippage_bps,
        )
        position, cash, closed_trade = _close_position(
            position,
            cash,
            last_timestamp,
            exit_price,
            "end_of_test",
            execution,
            len(df) - 1,
        )
        trades.append(closed_trade)
        if equity_points:
            equity_points[-1]["cash"] = cash
            equity_points[-1]["equity"] = cash
            equity_points[-1]["position_open"] = False

    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        trades_df = trades_df.sort_values("entry_time").reset_index(drop=True)

    equity_df = pd.DataFrame(equity_points)
    if not equity_df.empty:
        equity_df["time"] = pd.to_datetime(equity_df["time"])

    monthly_returns = _build_monthly_returns(equity_df, execution.initial_cash)
    summary = _build_summary(strategy, execution, symbol, trades_df, equity_df, monthly_returns)
    return BacktestResult(summary=summary, trades=trades_df, equity_curve=equity_df, monthly_returns=monthly_returns)
