"""Risk manager — ATR-based position sizing with min/max constraints."""

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)


def floor_to_step(qty: Decimal, step_size: int) -> Decimal:
    """Round DOWN to nearest step. step_size = decimal-places int."""
    factor = 10 ** step_size
    return Decimal((qty * factor).__floor__()) / factor


@dataclass(frozen=True)
class PositionSizing:
    quantity: Decimal
    stop_price: Decimal
    take1_price: Decimal
    take2_price: Decimal
    risk_amount: float
    notional: float


class RiskManager:
    """ATR-based risk and position sizing."""

    def __init__(
        self,
        equity: float,
        risk_per_trade_pct: float = 0.01,
        leverage: int = 10,
        max_positions: int = 3,
        max_symbol_notional_pct: float = 0.15,
        stop_atr: float = 1.5,
        take1_atr: float = 2.0,
        take2_atr: float = 4.0,
    ):
        self.equity = equity
        self.risk_per_trade_pct = risk_per_trade_pct
        self.leverage = leverage
        self.max_positions = max_positions
        self.max_symbol_notional_pct = max_symbol_notional_pct
        self.stop_atr = stop_atr
        self.take1_atr = take1_atr
        self.take2_atr = take2_atr

    def compute_position_size(
        self, symbol: str, direction: int, atr: float,
        entry_price: float, symbol_info, signal_score: float = 0.5,
    ) -> PositionSizing | None:
        """Compute position size. Returns None if rejected."""
        if atr <= 0 or entry_price <= 0:
            return None

        # Risk amount
        risk_amount = self.equity * self.risk_per_trade_pct

        # Stop distance
        stop_distance = atr * self.stop_atr
        tick_value = 10 ** -symbol_info.tick_size if symbol_info.tick_size > 0 else 0.00001
        if stop_distance < 2 * tick_value:
            logger.warning("Reject %s: stop_distance too small", symbol)
            return None

        # Raw quantity
        raw_qty = Decimal(str(risk_amount)) / Decimal(str(stop_distance))
        qty = floor_to_step(raw_qty, symbol_info.step_size)

        # Bump to min_qty
        min_qty = Decimal(str(symbol_info.min_qty))
        if qty < min_qty:
            qty = min_qty
            logger.info("Bumped %s to min_qty=%.8f", symbol, float(qty))

        # Cap to max_qty
        if symbol_info.max_qty > 0 and float(qty) > symbol_info.max_qty:
            qty = floor_to_step(Decimal(str(symbol_info.max_qty)), symbol_info.step_size)

        # Ensure min_notional (x1.15 for safety)
        notional = float(qty) * entry_price
        min_notional = symbol_info.min_notional * 1.15
        if notional < min_notional and entry_price > 0:
            needed = Decimal(str(min_notional)) / Decimal(str(entry_price))
            qty = floor_to_step(needed, symbol_info.step_size)
            logger.info("Bumped %s to meet min_notional: %.8f", symbol, float(qty))

        # Cap to max_notional
        max_notional = self.equity * self.max_symbol_notional_pct
        if entry_price > 0:
            max_qty = floor_to_step(Decimal(str(max_notional)) / Decimal(str(entry_price)), symbol_info.step_size)
            if qty > max_qty:
                qty = max_qty

        notional = float(qty) * entry_price

        # SL/TP levels
        d = Decimal(str(direction))
        a = Decimal(str(atr))
        stop_p = Decimal(str(entry_price)) - d * a * Decimal(str(self.stop_atr))
        take1_p = Decimal(str(entry_price)) + d * a * Decimal(str(self.take1_atr))
        take2_p = Decimal(str(entry_price)) + d * a * Decimal(str(self.take2_atr))

        return PositionSizing(
            quantity=qty,
            stop_price=stop_p,
            take1_price=take1_p,
            take2_price=take2_p,
            risk_amount=risk_amount,
            notional=notional,
        )

    def can_open_new(self, open_count: int) -> bool:
        return open_count < self.max_positions

    def update_equity(self, new_equity: float):
        self.equity = new_equity
