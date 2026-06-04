"""Risk manager — adaptive position sizing + portfolio limits.
Production params validated on 300+ portfolio configs + 4320 signal configs.
"""
from __future__ import annotations

import math
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PositionSizing:
    """Result of position sizing calculation."""
    quantity: float
    stop_price: float
    take1_price: float
    take2_price: float
    risk_amount: float
    risk_multiplier: float
    initial_risk: float  # $ at risk
    # ATR multipliers — stored so position_manager can recalculate
    # stop/take from the ACTUAL fill price (not the estimated signal close).
    stop_atr_mult: float = 1.5
    take1_atr_mult: float = 2.0
    take2_atr_mult: float = 4.0


class RiskManager:
    """Manages position sizing and portfolio risk."""

    def __init__(self, config: dict):
        self.risk_per_trade = config.get("risk_per_trade", 0.01)      # 1%
        self.max_positions = config.get("max_positions", 5)
        self.max_entries_per_bar = config.get("max_entries_per_bar", 2)
        self.cooldown_bars = config.get("symbol_cooldown", 3)
        self.stop_atr = config.get("stop_atr", 1.5)
        self.take1_atr = config.get("take1_atr", 2.0)
        self.take2_atr = config.get("take2_atr", 4.0)
        self.partial_exit_pct = config.get("partial_exit_pct", 0.1)

        # Adaptive sizing
        self.use_adaptive = config.get("use_adaptive_sizing", True)
        self.adaptive_min = config.get("adaptive_min_mult", 0.5)
        self.adaptive_max = config.get("adaptive_max_mult", 1.5)

    def can_open_new(self, open_count: int, entries_this_bar: int) -> bool:
        """Check if we can open a new position."""
        if open_count >= self.max_positions:
            logger.info(f"Max positions reached ({open_count}/{self.max_positions})")
            return False
        if entries_this_bar >= self.max_entries_per_bar:
            logger.info(f"Max entries this bar reached ({entries_this_bar}/{self.max_entries_per_bar})")
            return False
        return True

    def is_on_cooldown(self, last_close_ms: int | None, current_ms: int, bar_duration_ms: int) -> bool:
        """Check if symbol is on cooldown (cooldown_bars since last close)."""
        if last_close_ms is None:
            return False
        bars_since = (current_ms - last_close_ms) / bar_duration_ms
        if bars_since < self.cooldown_bars:
            logger.info(f"Symbol on cooldown ({bars_since:.1f}/{self.cooldown_bars} bars)")
            return True
        return False

    def compute_adaptive_multiplier(self, signal_score: float) -> float:
        """Compute risk multiplier based on signal score.

        score=0.25 → min_mult (0.5x)
        score=0.70 → max_mult (1.5x)
        Linear interpolation between min and max.
        """
        if not self.use_adaptive:
            return 1.0

        # Normalize score to [0, 1] range
        score_min, score_max = 0.25, 0.70
        t = max(0.0, min(1.0, (signal_score - score_min) / (score_max - score_min)))
        return self.adaptive_min + t * (self.adaptive_max - self.adaptive_min)

    def compute_position_size(
        self,
        equity: float,
        atr: float,
        signal_score: float,
        entry_price: float,
        direction: int,
        step_size: float,
        tick_size: int,
        min_notional: float,
        min_qty: float,
        max_qty: float = None,
        max_notional: float = None,
    ) -> PositionSizing | None:
        """Calculate position size with adaptive risk.

        Returns PositionSizing or None if position too small.
        """
        # Stop/take prices
        stop_distance = atr * self.stop_atr
        if direction == 1:  # LONG
            stop_price = entry_price - stop_distance
            take1_price = entry_price + atr * self.take1_atr
            take2_price = entry_price + atr * self.take2_atr
        else:  # SHORT
            stop_price = entry_price + stop_distance
            take1_price = entry_price - atr * self.take1_atr
            take2_price = entry_price - atr * self.take2_atr

        # Round prices (tick_size is decimal precision, e.g. 2 means 2 decimal places)
        tick_size = max(0, tick_size)  # guard: negative tick_size would corrupt prices
        stop_price = round(stop_price, tick_size)
        take1_price = round(take1_price, tick_size)
        take2_price = round(take2_price, tick_size)

        # Adaptive risk
        multiplier = self.compute_adaptive_multiplier(signal_score)
        risk_amount = equity * self.risk_per_trade * multiplier

        # Quantity
        if stop_distance <= 0:
            logger.warning(f"Stop distance <= 0, skipping")
            return None

        quantity = risk_amount / stop_distance

        # Round down to step_size precision (step_size is int = decimal places)
        if step_size >= 0:
            factor = 10 ** step_size
            quantity = int(quantity * factor) / factor

        # Cap at exchange max_qty (guard: max_qty=0 means invalid/unset, not "zero allowed")
        if max_qty is not None and max_qty > 0 and quantity > max_qty:
            logger.info(f"Quantity {quantity} capped to max_qty {max_qty}")
            quantity = max_qty
            if step_size >= 0:
                factor = 10 ** step_size
                quantity = int(quantity * factor) / factor

        # Check minimums
        if quantity <= 0 or quantity < min_qty:
            logger.warning(f"Quantity {quantity} below min_qty {min_qty}")
            return None

        notional = quantity * entry_price
        if notional < min_notional:
            # Try min notional
            quantity = min_notional * 1.1 / entry_price
            if step_size >= 0:
                factor = 10 ** step_size
                quantity = math.ceil(quantity * factor) / factor

        # Cap by max notional (leverage bracket limit — prevents -2027)
        if max_notional and (quantity * entry_price) > max_notional:
            quantity = max_notional / entry_price
            if step_size >= 0:
                factor = 10 ** step_size
                quantity = int(quantity * factor) / factor
            logger.info(f"Notional capped to max_notional={max_notional:.0f} → qty={quantity}")

        initial_risk = abs(entry_price - stop_price) * quantity

        # Safety guard: reject if min_notional fallback pushed risk > 2x target
        if initial_risk > risk_amount * 2.0:
            logger.warning(
                f"Risk guard: initial_risk={initial_risk:.2f} > 2x target={risk_amount:.2f}, skipping"
            )
            return None

        return PositionSizing(
            quantity=quantity,
            stop_price=stop_price,
            take1_price=take1_price,
            take2_price=take2_price,
            risk_amount=risk_amount,
            risk_multiplier=multiplier,
            initial_risk=initial_risk,
            stop_atr_mult=self.stop_atr,
            take1_atr_mult=self.take1_atr,
            take2_atr_mult=self.take2_atr,
        )
