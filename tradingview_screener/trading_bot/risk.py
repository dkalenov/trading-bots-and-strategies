"""
Risk manager - position sizing and the stop/take1/take2 levels for a
trade.

Sizing formula here is fixed-notional-per-trade (order_size_usd / price,
capped to the symbol's step size and bumped up to min notional if
needed), because that's what the real settings snapshots from the
original project's database actually show (see docs/AUDIT.md, H2) -
not a risk-percentage-of-equity formula. Copying a different sizing
model just because it looks more sophisticated would reintroduce
exactly the kind of "formula doesn't match what was actually audited"
mismatch this whole rebuild exists to fix.
"""
from __future__ import annotations

import sys
import os
from dataclasses import dataclass
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import StrategyConfig
from models import ExitLevels, PositionSizing, SymbolFilters
from utils import quantize_down, quantize_up, quantize_price


def decide_entry(signal: str, btc_signal: str) -> str | None:
    """process_trade_signal()'s open_long / open_short condition, ported
    from main.py. Lives here rather than in strategies/ because it's pure
    risk/eligibility logic with no TradingView-specific fetching in it -
    strategies/tradingview_screener.py calls this after it has both
    ratings in hand."""
    from models import Direction
    BTC_OK_FOR_LONG = ("STRONG_BUY", "BUY", "NEUTRAL")
    BTC_OK_FOR_SHORT = ("STRONG_SELL", "SELL", "NEUTRAL")
    if signal == "STRONG_BUY" and btc_signal in BTC_OK_FOR_LONG:
        return Direction.LONG.value
    if signal == "STRONG_SELL" and btc_signal in BTC_OK_FOR_SHORT:
        return Direction.SHORT.value
    return None


class RiskManager:
    """Computes position size and exit levels for one trade. Stateless -
    every call is independent, given a config and the current market
    facts (ATR, entry price, symbol filters)."""

    def __init__(self, config: StrategyConfig):
        self._config = config

    def compute_exit_levels(self, direction: str, entry_price: float, atr: float) -> ExitLevels:
        """Exactly the stop/take1/take2 formula in the original new_trade()."""
        from models import Direction
        cfg = self._config
        if direction == Direction.LONG.value:
            raw = ExitLevels(
                stop=Decimal(str(entry_price - atr * cfg.stop_mult)),
                take1=Decimal(str(entry_price + atr * cfg.take1_mult)),
                take2=Decimal(str(entry_price + atr * cfg.take2_mult)),
            )
        elif direction == Direction.SHORT.value:
            raw = ExitLevels(
                stop=Decimal(str(entry_price + atr * cfg.stop_mult)),
                take1=Decimal(str(entry_price - atr * cfg.take1_mult)),
                take2=Decimal(str(entry_price - atr * cfg.take2_mult)),
            )
        else:
            raise ValueError(f"direction must be LONG or SHORT, got {direction!r}")
        return raw

    def breakeven_stop_price(self, direction: str, entry_price: float) -> float:
        """Exactly the `entry_price * (0.999 if BUY else 1.001)` line in
        the original partial_close_and_move_stop()."""
        from models import Direction
        buf = self._config.breakeven_buffer
        if direction == Direction.LONG.value:
            return entry_price * (1 - buf)
        return entry_price * (1 + buf)

    def compute_position_size(self, entry_price: float, atr: float, direction: str,
                               filters: SymbolFilters) -> PositionSizing:
        """The one and only position-sizing call site. Rounds quantity
        down to step_size and prices to tick_size through utils.py's
        Decimal-based helpers - see docs/AUDIT.md T1/T3 for why plain
        float rounding isn't safe here."""
        levels = self.compute_exit_levels(direction, entry_price, atr)

        qty = self._config.order_size_usd / entry_price
        if filters.step_size:
            qty = quantize_down(qty, filters.step_size)
        if filters.min_notional and qty * entry_price < filters.min_notional * 1.1:
            qty = quantize_up(filters.min_notional * 1.1 / entry_price, filters.step_size or 0.0)

        return PositionSizing(
            quantity=Decimal(str(qty)),
            entry_price=Decimal(str(entry_price)),
            stop=Decimal(str(quantize_price(float(levels.stop), filters.tick_size))),
            take1=Decimal(str(quantize_price(float(levels.take1), filters.tick_size))),
            take2=Decimal(str(quantize_price(float(levels.take2), filters.tick_size))),
            atr=atr,
        )
