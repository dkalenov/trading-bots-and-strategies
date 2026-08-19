"""
The strategy rules, extracted once from main.py / trading_bot/main.py and
used identically by the backtester and the bot. Nothing here talks to a
network, a database, or an exchange - it's pure functions over plain values,
which is what makes it possible to unit-test and to guarantee backtest and
live can never quietly diverge again.

Source of the rules (see /docs/AUDIT.md for the full trace):
  - process_trade_signal() in main.py           -> entry decision
  - new_trade() in main.py                       -> stop/take1/take2 levels
  - partial_close_and_move_stop() in main.py     -> take1 partial + breakeven
"""
from __future__ import annotations
from dataclasses import dataclass


LONG = "LONG"
SHORT = "SHORT"

# Signals TradingView's technical rating can return.
STRONG_BUY, BUY, NEUTRAL, SELL, STRONG_SELL = (
    "STRONG_BUY", "BUY", "NEUTRAL", "SELL", "STRONG_SELL",
)

BTC_OK_FOR_LONG = (STRONG_BUY, BUY, NEUTRAL)
BTC_OK_FOR_SHORT = (STRONG_SELL, SELL, NEUTRAL)


@dataclass(frozen=True)
class StrategyConfig:
    atr_length: int = 14
    stop_mult: float = 0.45
    take1_mult: float = 2.5
    take2_mult: float = 5.0
    take1_portion: float = 0.05      # fraction of the position closed at take1
    order_size_usd: float = 10.0     # fixed notional per trade - see README
    breakeven_buffer: float = 0.001  # matches entry*0.999 / entry*1.001 in the original


def decide_entry(signal: str, btc_signal: str) -> str | None:
    """
    Exactly process_trade_signal()'s open_long / open_short condition.
    Returns LONG, SHORT, or None. No side effects, no state.
    """
    if signal == STRONG_BUY and btc_signal in BTC_OK_FOR_LONG:
        return LONG
    if signal == STRONG_SELL and btc_signal in BTC_OK_FOR_SHORT:
        return SHORT
    return None


@dataclass(frozen=True)
class ExitLevels:
    stop: float
    take1: float
    take2: float


def compute_exit_levels(direction: str, entry_price: float, atr: float,
                         cfg: StrategyConfig = StrategyConfig()) -> ExitLevels:
    """Exactly the stop/take1/take2 formula in new_trade()."""
    if direction == LONG:
        return ExitLevels(
            stop=entry_price - atr * cfg.stop_mult,
            take1=entry_price + atr * cfg.take1_mult,
            take2=entry_price + atr * cfg.take2_mult,
        )
    if direction == SHORT:
        return ExitLevels(
            stop=entry_price + atr * cfg.stop_mult,
            take1=entry_price - atr * cfg.take1_mult,
            take2=entry_price - atr * cfg.take2_mult,
        )
    raise ValueError(f"direction must be {LONG!r} or {SHORT!r}, got {direction!r}")


def breakeven_stop_price(direction: str, entry_price: float,
                          cfg: StrategyConfig = StrategyConfig()) -> float:
    """Exactly the `entry_price * (0.999 if BUY else 1.001)` line in
    partial_close_and_move_stop(). Kept as one function so the 0.999/1.001
    constant only ever lives in one place."""
    if direction == LONG:
        return entry_price * (1 - cfg.breakeven_buffer)
    return entry_price * (1 + cfg.breakeven_buffer)


def position_size(order_size_usd: float, price: float,
                   step_size: float | None = None,
                   min_notional: float | None = None) -> float:
    """
    The one and only position-sizing formula in the project.
    Mirrors utils.round_down(order_size / price, step_size) plus the
    min-notional bump-up in new_trade() - both backtest and bot call this,
    so a fix here fixes both, and a bug here is a bug in exactly one place.
    """
    qty = order_size_usd / price
    if step_size:
        qty = _round_down(qty, step_size)
    if min_notional and qty * price < min_notional * 1.1:
        qty = _round_up(min_notional * 1.1 / price, step_size or 0.0)
    return qty


def _round_down(value: float, step: float) -> float:
    if step <= 0:
        return value
    return (int(value / step)) * step


def _round_up(value: float, step: float) -> float:
    if step <= 0:
        return value
    import math
    return math.ceil(value / step) * step
