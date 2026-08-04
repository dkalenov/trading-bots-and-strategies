"""Symbol trading filters (step size, min qty, min notional, ...) and
the rounding helpers position sizing and order placement both need.

Binance returns these as strings inside exchangeInfo's per-symbol
`filters` list; this module extracts the ones we care about into one
plain, easy-to-use object.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    step_size: Decimal          # LOT_SIZE stepSize — quantity must be a multiple of this
    min_qty: Decimal            # LOT_SIZE minQty
    max_qty: Decimal            # LOT_SIZE maxQty
    tick_size: Decimal          # PRICE_FILTER tickSize — price must be a multiple of this
    min_notional: Decimal       # MIN_NOTIONAL notional
    quantity_precision: int
    price_precision: int

    @classmethod
    def from_exchange_info_symbol(cls, sym: dict) -> "SymbolFilters":
        filters = {f["filterType"]: f for f in sym.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        price = filters.get("PRICE_FILTER", {})
        notional = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})
        return cls(
            symbol=sym["symbol"],
            step_size=Decimal(str(lot.get("stepSize", "0.001"))),
            min_qty=Decimal(str(lot.get("minQty", "0.001"))),
            max_qty=Decimal(str(lot.get("maxQty", "1000000"))),
            tick_size=Decimal(str(price.get("tickSize", "0.01"))),
            min_notional=Decimal(str(notional.get("notional", "5"))),
            quantity_precision=int(sym.get("quantityPrecision", 3)),
            price_precision=int(sym.get("pricePrecision", 2)),
        )


def round_step(value: Decimal, step: Decimal) -> Decimal:
    """Round DOWN to the nearest multiple of `step` (never round up a
    quantity — that could push notional/risk above what was intended)."""
    if step == 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def round_price(value: Decimal, tick: Decimal) -> Decimal:
    """Round a price to the nearest valid tick (nearest, not down —
    prices aren't a risk-sizing concern the way quantity is)."""
    if tick == 0:
        return value
    steps = (value / tick).to_integral_value()
    return steps * tick
