"""
The layer between PositionManager (policy: what should happen) and the
exchange client (mechanism: how to make it happen). Keeping this
separate means position_manager.py's tests can substitute a fake
OrderManager without needing a fake that mimics Binance's full REST
surface.
"""
from __future__ import annotations
import logging

log = logging.getLogger("order_manager")

_VALID_TRANSITIONS = {
    None: {"submitted"},
    "submitted": {"filled", "rejected", "canceled"},
    "filled": set(),  # terminal
    "rejected": set(),
    "canceled": set(),
}


def is_valid_transition(current: str | None, target: str) -> bool:
    return target in _VALID_TRANSITIONS.get(current, set())


class OrderManager:
    """Places and cancels orders. dry_run=True logs what it would do and
    returns None ids instead of calling the exchange - the only branch
    point for dry-run vs live in the whole order-placement path."""

    def __init__(self, exchange, dry_run: bool = True):
        self.exchange = exchange
        self.dry_run = dry_run

    def place_entry_market(self, symbol: str, side: str, quantity: float) -> str | None:
        if self.dry_run:
            log.info("[DRY RUN] %s MARKET %s qty=%s", symbol, side, quantity)
            return None
        order = self.exchange.new_market_order(symbol, side, quantity)
        return str(order.get("orderId"))

    def place_stop_algo(self, symbol: str, side: str, trigger_price: float,
                         close_position: bool = True, quantity: float | None = None) -> str | None:
        if self.dry_run:
            log.info("[DRY RUN] %s STOP_MARKET(algo) %s trigger=%s close_position=%s qty=%s",
                      symbol, side, trigger_price, close_position, quantity)
            return None
        order = self.exchange.new_algo_stop_market_order(
            symbol, side, trigger_price, close_position=close_position, quantity=quantity)
        return str(order.get("algoId"))

    def place_take_limit(self, symbol: str, side: str, price: float, quantity: float) -> str | None:
        if self.dry_run:
            log.info("[DRY RUN] %s LIMIT %s price=%s qty=%s", symbol, side, price, quantity)
            return None
        order = self.exchange.new_limit_order(symbol, side, price, quantity, reduce_only=True)
        return str(order.get("orderId"))

    def place_reduce_market(self, symbol: str, side: str, quantity: float) -> None:
        if self.dry_run:
            log.info("[DRY RUN] %s MARKET reduceOnly %s qty=%s", symbol, side, quantity)
            return
        self.exchange.new_market_order(symbol, side, quantity, reduce_only=True)

    def cancel_stop_algo(self, symbol: str, algo_id: str | None) -> None:
        if self.dry_run or not algo_id:
            return
        try:
            self.exchange.cancel_algo_order(symbol, algo_id)
        except Exception:
            log.warning("could not cancel algo stop %s for %s", algo_id, symbol)

    def cancel_take_limit(self, symbol: str, order_id: str | None) -> None:
        if self.dry_run or not order_id:
            return
        try:
            self.exchange.cancel_order(symbol, order_id)
        except Exception:
            log.warning("could not cancel take2 order %s for %s", order_id, symbol)
