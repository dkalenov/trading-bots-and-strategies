"""
Tracks whether a position's protective orders (stop, take2) are believed
to be resting on the exchange, and carries out the one state transition
this strategy needs: take1 touched -> partial close -> stop moved to
breakeven.

Simplified from the reference architecture's ProtectionManager, which
tracks a fuller state machine (PLACING/PROTECTED/MISSING/FAILED/...) with
cache-first REST verification and retry backoff for a multi-strategy,
multi-symbol production system watching hundreds of positions. This
project tracks two states per open trade - protected by an initial stop,
or protected by a breakeven stop after take1 - because that's the entire
state space this one strategy has. If a future strategy in this repo
needs richer protection state, that's the natural place to extend this
class, not a reason to have built the fuller machine speculatively now.
"""
from __future__ import annotations
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models import PositionState
from risk import RiskManager
from utils import quantize_down, quantize_price

log = logging.getLogger("protection")


class ProtectionManager:
    def __init__(self, order_manager, risk_manager: RiskManager):
        self.orders = order_manager
        self.risk = risk_manager
        self._state: dict[str, PositionState] = {}

    def state_of(self, symbol: str) -> PositionState | None:
        return self._state.get(symbol)

    def mark_open(self, symbol: str) -> None:
        self._state[symbol] = PositionState.OPEN

    def mark_protected(self, symbol: str) -> None:
        self._state[symbol] = PositionState.PROTECTED

    def mark_closed(self, symbol: str) -> None:
        self._state.pop(symbol, None)

    def handle_take1(self, trade, scfg) -> dict:
        """trade: a models.TradeRecord with take1_done=False. Closes
        `take1_portion` of the position at market, cancels the original
        stop, places a new breakeven stop for the remainder - and does
        the same for the take2 limit order, which otherwise keeps
        resting at the full pre-take1 quantity. Left unchanged, that
        stale take2 order is oversized against what's actually left in
        the position: Binance rejects a reduce-only order larger than
        the position with "reduce-only quantity exceeds position", or
        (worse) partially fills it and leaves the qty accounting wrong
        for the market-close that runs when the position finally exits.

        Quantities are rounded down to the symbol's step size and the
        breakeven stop trigger price is rounded to the tick size so the
        exchange never rejects them with -1111 / -4014.
        """
        long = trade.direction == "LONG"
        filters = self.orders.exchange.get_symbol_filters(trade.symbol)
        step = getattr(filters, "step_size", 0.0) or 0.0
        tick = getattr(filters, "tick_size", 0.0) or 0.0
        close_qty = quantize_down(trade.qty_full * scfg.take1_portion, step)
        remaining = quantize_down(trade.qty_remaining - close_qty, step)
        be_stop = quantize_price(self.risk.breakeven_stop_price(trade.direction, trade.entry_price), tick)

        side = "SELL" if long else "BUY"
        if close_qty > 0:
            self.orders.place_reduce_market(trade.symbol, side, close_qty)
        else:
            log.warning("%s take1 hit but close_qty rounded to 0 (step=%s, qty=%s) - skipping partial close",
                        trade.symbol, step, trade.qty_full)

        self.orders.cancel_stop_algo(trade.symbol, trade.stop_order_id)
        new_stop_id = self.orders.place_stop_algo(
            trade.symbol, side, be_stop, close_position=False, quantity=remaining)

        self.orders.cancel_take_limit(trade.symbol, trade.take2_order_id)
        new_take2_id = self.orders.place_take_limit(trade.symbol, side, trade.take2, remaining)

        self.mark_protected(trade.symbol)
        log.info("%s take1 hit @ %.6f -> closed %.6f, stop moved to breakeven %.6f, "
                  "take2 order resized to %.6f",
                  trade.symbol, trade.take1, close_qty, be_stop, remaining)
        return dict(breakeven_stop=be_stop, qty_remaining=remaining,
                    stop_order_id=new_stop_id, take2_order_id=new_take2_id)

    def release(self, trade, reason: str) -> None:
        """Cancel whichever protective order didn't trigger the exit that
        just happened. The stop is always an algo order, take2 is always
        a plain LIMIT order - which cancel call to make depends on which
        side triggered, not on a single shared order id."""
        if reason == "TAKE2":
            self.orders.cancel_stop_algo(trade.symbol, trade.stop_order_id)
        else:  # STOP or BREAKEVEN_STOP: the stop side triggered, take2 is leftover
            self.orders.cancel_take_limit(trade.symbol, trade.take2_order_id)
        self.mark_closed(trade.symbol)
