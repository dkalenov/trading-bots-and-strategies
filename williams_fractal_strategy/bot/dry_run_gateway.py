"""
In-memory simulated exchange client — same async method surface as
gateway.Gateway (new_order, new_algo_order, cancel_order, open_orders,
open_algo_orders, cancel_algo_order, change_leverage, position_risk,
listen key methods, ...), so the bot engine can use either one
interchangeably without an if/else anywhere in the trading logic itself.

Used for EXECUTION_MODE=dry_run: no network access at all, which is
also exactly what makes it possible to verify the bot's order-
placement logic (entry, stop, take-profit) from an environment with no
exchange connectivity — see scripts/verify_live_pipeline.py.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from decimal import Decimal

from filters import SymbolFilters


@dataclass
class SimOrder:
    order_id: int
    symbol: str
    side: str            # BUY | SELL
    type: str             # MARKET | LIMIT | STOP_MARKET | TAKE_PROFIT_MARKET
    stop_price: Decimal | None
    close_position: bool
    reduce_only: bool = False
    quantity: Decimal | None = None
    status: str = "NEW"   # NEW | FILLED | CANCELED


@dataclass
class SimPosition:
    symbol: str
    direction: int         # 1 long, -1 short, 0 flat
    entry_price: Decimal
    quantity: Decimal


class DryRunGateway:
    def __init__(self, starting_balance: Decimal = Decimal("10000")):
        self._balance = starting_balance
        self._positions: dict[str, SimPosition] = {}
        self._open_orders: dict[str, dict[int, SimOrder]] = {}
        self._prices: dict[str, Decimal] = {}
        self._filters: dict[str, SymbolFilters] = {}
        self._order_ids = itertools.count(1)
        self._leverage: dict[str, int] = {}

    async def __aenter__(self) -> "DryRunGateway":
        return self

    async def __aexit__(self, *exc) -> None:
        return None

    # ---- test/demo helpers (not part of the real client's interface) ----

    def set_symbol_filters(self, symbol: str, filters: SymbolFilters) -> None:
        self._filters[symbol] = filters

    def update_price(self, symbol: str, price: Decimal) -> list[dict]:
        """Feed a new price; returns fill events for any triggered STOP/TAKE orders."""
        self._prices[symbol] = Decimal(str(price))
        return self._check_triggers(symbol)

    def _check_triggers(self, symbol: str) -> list[dict]:
        events = []
        price = self._prices.get(symbol)
        pos = self._positions.get(symbol)
        if price is None or pos is None or pos.direction == 0:
            return events

        for oid, o in list(self._open_orders.get(symbol, {}).items()):
            if o.status != "NEW" or o.stop_price is None:
                continue
            is_close = o.close_position or o.reduce_only
            triggered = False
            if o.type == "STOP_MARKET" and is_close:
                triggered = (
                    (pos.direction == 1 and price <= o.stop_price) or
                    (pos.direction == -1 and price >= o.stop_price)
                )
            elif o.type == "TAKE_PROFIT_MARKET" and is_close:
                triggered = (
                    (pos.direction == 1 and price >= o.stop_price) or
                    (pos.direction == -1 and price <= o.stop_price)
                )
            elif o.type == "LIMIT" and is_close:
                triggered = (
                    (pos.direction == 1 and price >= o.stop_price) or
                    (pos.direction == -1 and price <= o.stop_price)
                )
            if triggered:
                o.status = "FILLED"
                self._open_orders[symbol].pop(oid, None)
                for other_id, other in list(self._open_orders.get(symbol, {}).items()):
                    other.status = "CANCELED"
                self._open_orders[symbol] = {}
                close_qty = o.quantity if o.quantity and o.quantity > 0 else pos.quantity
                pnl = (price - pos.entry_price) * close_qty * pos.direction
                self._balance += pnl
                self._positions[symbol] = SimPosition(symbol, 0, Decimal("0"), Decimal("0"))
                events.append({
                    "orderId": oid, "symbol": symbol, "type": o.type,
                    "side": o.side, "price": str(price), "pnl": str(pnl),
                    "event": "PROTECTION_FILLED",
                })
        return events

    # ---- Gateway-compatible interface ----

    async def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        if symbol not in self._filters:
            raise ValueError(f"no simulated filters set for {symbol} (call set_symbol_filters first)")
        return self._filters[symbol]

    async def usdt_equity(self) -> Decimal:
        return self._balance

    async def change_leverage(self, symbol: str, leverage: int) -> dict:
        self._leverage[symbol] = leverage
        return {"symbol": symbol, "leverage": leverage}

    async def position_risk(self, symbol: str) -> list[dict]:
        pos = self._positions.get(symbol)
        if pos is None or pos.direction == 0:
            return [{"symbol": symbol, "positionAmt": "0", "entryPrice": "0"}]
        return [{
            "symbol": symbol,
            "positionAmt": str(pos.quantity * pos.direction),
            "entryPrice": str(pos.entry_price),
        }]

    async def all_positions(self) -> list[dict]:
        out = []
        for symbol, pos in self._positions.items():
            if pos.direction != 0:
                out.append({
                    "symbol": symbol, "positionAmt": str(pos.quantity * pos.direction),
                    "entryPrice": str(pos.entry_price),
                })
        return out

    async def new_order(self, **params) -> dict:
        result = self._place_sim_order(**params)
        return {"orderId": result["orderId"], "symbol": result["symbol"],
                "side": result["side"], "type": result["type"],
                "status": result["status"], "stopPrice": result.get("stopPrice", ""),
                "closePosition": "true" if result.get("close_position") else "false",
                "reduceOnly": "true" if result.get("reduce_only") else "false"}

    async def new_algo_order(self, **params) -> dict:
        result = self._place_sim_order(**params)
        return {"algoId": result["orderId"], "symbol": result["symbol"],
                "side": result["side"], "type": result["type"],
                "algoType": "CONDITIONAL", "status": result["status"],
                "stopPrice": result.get("stopPrice", ""), "triggerPrice": result.get("stopPrice", ""),
                "quantity": params.get("quantity", ""), "reduceOnly": "true",
                "workingType": params.get("workingType", "CONTRACT_PRICE"),
                "positionSide": params.get("positionSide", "BOTH")}

    def _place_sim_order(self, **params) -> dict:
        symbol = params["symbol"]
        side = params["side"]
        order_type = params["type"]
        oid = next(self._order_ids)
        price = self._prices.get(symbol, Decimal("0"))

        if order_type == "MARKET":
            qty = Decimal(str(params["quantity"]))
            direction = 1 if side == "BUY" else -1
            existing = self._positions.get(symbol)
            if existing and existing.direction != 0 and existing.direction != direction:
                pnl = (price - existing.entry_price) * existing.quantity * existing.direction
                self._balance += pnl
                self._positions[symbol] = SimPosition(symbol, direction, price, qty)
            else:
                self._positions[symbol] = SimPosition(symbol, direction, price, qty)
            self._open_orders.setdefault(symbol, {})
            return {
                "orderId": oid, "symbol": symbol, "side": side, "type": order_type,
                "status": "FILLED", "avgPrice": str(price), "executedQty": str(qty),
            }

        stop_price = Decimal(str(params.get("stopPrice") or params.get("triggerPrice") or params.get("price", "0")))
        close_position = params.get("closePosition") == "true"
        reduce_only = params.get("reduceOnly") == "true"
        qty = Decimal(str(params.get("quantity", "0")))
        is_algo = params.get("algoType") == "CONDITIONAL"
        order = SimOrder(
            order_id=oid, symbol=symbol, side=side, type=order_type,
            stop_price=stop_price, close_position=close_position,
            reduce_only=reduce_only or is_algo, quantity=qty if qty > 0 else None,
        )
        self._open_orders.setdefault(symbol, {})[oid] = order
        self._check_triggers(symbol)
        return {"orderId": oid, "symbol": symbol, "side": side, "type": order_type,
                "status": "NEW", "stopPrice": str(stop_price)}

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        o = self._open_orders.get(symbol, {}).pop(order_id, None)
        if o:
            o.status = "CANCELED"
        return {"symbol": symbol, "orderId": order_id, "status": "CANCELED"}

    async def cancel_all_open_orders(self, symbol: str) -> dict:
        for o in self._open_orders.get(symbol, {}).values():
            o.status = "CANCELED"
        self._open_orders[symbol] = {}
        return {"symbol": symbol, "status": "CANCELED_ALL"}

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        out = []
        symbols = [symbol] if symbol else list(self._open_orders.keys())
        for sym in symbols:
            for o in self._open_orders.get(sym, {}).values():
                if o.status == "NEW":
                    out.append({
                        "orderId": o.order_id, "symbol": o.symbol, "side": o.side,
                        "type": o.type, "stopPrice": str(o.stop_price), "status": o.status,
                        "closePosition": "true" if o.close_position else "false",
                        "reduceOnly": "true" if o.reduce_only else "false",
                    })
        return out

    async def open_algo_orders(self, symbol: str | None = None) -> list[dict]:
        return await self.open_orders(symbol)

    async def cancel_algo_order(self, symbol: str, algo_id: int) -> dict:
        o = self._open_orders.get(symbol, {}).pop(algo_id, None)
        if o:
            o.status = "CANCELED"
        return {"symbol": symbol, "algoId": algo_id, "status": "CANCELED"}

    async def start_listen_key(self) -> str:
        return "dryrun-listen-key"

    async def keepalive_listen_key(self) -> None:
        return None

    async def close_listen_key(self) -> None:
        return None

    async def wait_for_order_fill(self, symbol: str, order_id: int, timeout: float = 30.0) -> dict:
        o = self._open_orders.get(symbol, {}).get(order_id)
        if o and o.status == "FILLED":
            return {"orderId": order_id, "status": "FILLED", "avgPrice": str(self._prices.get(symbol, "0"))}
        for oid, o in list(self._open_orders.get(symbol, {}).items()):
            if o.status == "FILLED":
                return {"orderId": oid, "status": "FILLED", "avgPrice": str(self._prices.get(symbol, "0"))}
        return {"orderId": order_id, "status": "FILLED", "avgPrice": str(self._prices.get(symbol, "0"))}
