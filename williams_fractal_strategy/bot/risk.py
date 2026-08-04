"""
Position sizing and stop/take-profit price calculation.

Deliberately mirrors ../src/backtest.py's `_resolve_stop_price` /
sizing logic parameter-for-parameter (risk_per_trade, stop_mode,
atr_multiplier, stop_pct, reward_risk_ratio, min/max_stop_pct) so a
live run and a backtest run with the same config make the same
decisions given the same prices — this bot is not a separate strategy
implementation, it's the backtested one wired to a real exchange.

On top of that (which the backtest doesn't need, since it isn't
constrained by real order-book rules), this module rounds the
computed quantity to the exchange's stepSize and bumps it up to
min_qty / min_notional when needed — the same bump-not-skip approach
the algofactory_bot reference project's risk.py uses, confirmed
empirically to work correctly (see that project's VERIFICATION_NOTES.md).
"""
from __future__ import annotations

from decimal import Decimal

from filters import SymbolFilters, round_price, round_step
from models import SizingResult


def resolve_stop_price(
    direction: int,
    entry_price: Decimal,
    structure_stop: Decimal | None,
    stop_mode: str,
    atr_value: Decimal | None,
    atr_multiplier: float,
    stop_pct: float,
    min_stop_pct: float,
    max_stop_pct: float,
) -> Decimal:
    """Same logic as backtest.py's _resolve_stop_price — see that
    module's docstring for the fallback chain and clipping rationale."""
    stop_price = None

    if stop_mode == "structure" and structure_stop is not None:
        stop_price = structure_stop
    elif stop_mode == "atr" and atr_value is not None and atr_value > 0:
        stop_price = entry_price - Decimal(direction) * Decimal(str(atr_multiplier)) * atr_value
    elif stop_mode == "percent":
        stop_price = entry_price * (1 - Decimal(direction) * Decimal(str(stop_pct)))

    if stop_price is None:
        if atr_value is not None and atr_value > 0:
            stop_price = entry_price - Decimal(direction) * Decimal(str(atr_multiplier)) * atr_value
        else:
            stop_price = entry_price * (1 - Decimal(direction) * Decimal(str(stop_pct)))

    dist_pct = abs(entry_price - stop_price) / entry_price
    dist_pct = min(max(dist_pct, Decimal(str(min_stop_pct))), Decimal(str(max_stop_pct)))
    return entry_price - Decimal(direction) * dist_pct * entry_price


def compute_sizing(
    *,
    direction: int,
    entry_price: Decimal,
    equity: Decimal,
    filters: SymbolFilters,
    structure_stop: Decimal | None,
    atr_value: Decimal | None,
    risk_per_trade: float,
    stop_mode: str,
    atr_multiplier: float,
    stop_pct: float,
    reward_risk_ratio: float,
    max_leverage: float,
    min_stop_pct: float,
    max_stop_pct: float,
    debug_mode: bool = False,
) -> SizingResult:
    """Full pipeline: stop price -> take price -> risk-based quantity ->
    exchange rounding -> min_qty/min_notional bump -> max_leverage cap."""
    stop_price = resolve_stop_price(
        direction, entry_price, structure_stop, stop_mode,
        atr_value, atr_multiplier, stop_pct, min_stop_pct, max_stop_pct,
    )
    stop_price = round_price(stop_price, filters.tick_size)

    stop_distance = abs(entry_price - stop_price)
    take_price = entry_price + Decimal(direction) * Decimal(str(reward_risk_ratio)) * stop_distance
    take_price = round_price(take_price, filters.tick_size)

    risk_amount = equity * Decimal(str(risk_per_trade))
    stop_dist_pct = stop_distance / entry_price
    if stop_dist_pct <= 0:
        return SizingResult(Decimal("0"), stop_price, take_price, Decimal("0"), risk_amount,
                             rejected_reason="stop distance is zero")

    raw_qty = risk_amount / stop_dist_pct / entry_price
    max_qty_by_leverage = (equity * Decimal(str(max_leverage))) / entry_price
    qty = min(raw_qty, max_qty_by_leverage, filters.max_qty)

    if debug_mode:
        # keep debug-run sizes small and predictable — verify the
        # pipeline without risking a large position while doing it.
        qty = min(qty, filters.min_qty * Decimal("5"))

    qty = round_step(qty, filters.step_size)

    if qty < filters.min_qty:
        qty = round_step(filters.min_qty, filters.step_size)
        if qty < filters.min_qty:
            qty = filters.min_qty

    notional = qty * entry_price
    if notional < filters.min_notional:
        # bump up to comfortably clear min_notional (15% headroom absorbs
        # price movement between this calculation and the actual fill)
        needed_qty = (filters.min_notional * Decimal("1.15")) / entry_price
        qty = round_step(needed_qty, filters.step_size)
        while qty * entry_price < filters.min_notional:
            qty += filters.step_size
        notional = qty * entry_price

    if qty <= 0:
        return SizingResult(Decimal("0"), stop_price, take_price, Decimal("0"), risk_amount,
                             rejected_reason="computed quantity is zero after rounding")

    return SizingResult(qty, stop_price, take_price, notional, risk_amount)
