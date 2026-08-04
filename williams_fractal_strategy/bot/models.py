"""Shared dataclasses. Kept in one place so gateway.py, risk.py, db.py
and main.py all agree on the same shapes without importing each other."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class PositionState:
    """A currently-open position, as the bot understands it."""
    symbol: str
    direction: int              # 1 long, -1 short
    entry_price: str
    quantity: str
    stop_order_id: int | None = None
    take_order_id: int | None = None
    opened_at: str = ""


@dataclass
class SizingResult:
    """Output of risk.compute_sizing() — everything needed to place
    the entry + protection orders, or the reason it was rejected."""
    quantity: Decimal
    stop_price: Decimal
    take_price: Decimal
    notional: Decimal
    risk_amount: Decimal
    rejected_reason: str | None = None

    @property
    def accepted(self) -> bool:
        return self.rejected_reason is None


@dataclass
class TradeRecord:
    """A closed trade, for db.py's history table."""
    symbol: str
    direction: int
    entry_price: str
    exit_price: str
    quantity: str
    pnl: str
    exit_reason: str
    opened_at: str
    closed_at: str
