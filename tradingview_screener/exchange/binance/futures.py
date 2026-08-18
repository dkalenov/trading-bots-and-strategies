"""
USD-M Futures trading methods. Subclasses Client for the signed/public
request plumbing and adds everything the bot actually calls: klines,
mark price, exchange filters, leverage, orders.

Conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET, and friends) go
through the separate Algo Order API (/fapi/v1/algoOrder) as of Binance's
2025-12-09 migration - see new_algo_stop_market_order() below. Plain
MARKET and LIMIT orders are unaffected and still use /fapi/v1/order.
This split is documented in docs/AUDIT.md, finding T2.

Endpoint paths are current as of this writing (verified against
developers.binance.com on 2026-08-15) - Binance does version and migrate
this API over time, so if an endpoint starts returning 404 or a new
error code, check
https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api
before assuming the bot logic is wrong.
"""
from __future__ import annotations
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from exchange.binance.client import Client
from exchange.filters import parse_symbol_filters
from models import SymbolFilters


class Futures(Client):
    # ---- public/market data ----

    def get_klines(self, symbol: str, interval: str = "4h", limit: int = 200) -> list:
        return self.public_request("GET", "/fapi/v1/klines",
                                    {"symbol": symbol, "interval": interval, "limit": limit})

    def get_mark_price(self, symbol: str) -> float:
        data = self.public_request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data["markPrice"])

    def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        info = self.public_request("GET", "/fapi/v1/exchangeInfo")
        return parse_symbol_filters(info, symbol)

    # ---- account / trading (signed) ----

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return self.signed_request("POST", "/fapi/v1/leverage",
                                    {"symbol": symbol, "leverage": leverage})

    def get_position(self, symbol: str) -> dict | None:
        rows = self.signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
        for r in rows:
            if r["symbol"] == symbol and float(r["positionAmt"]) != 0:
                return r
        return None

    def new_market_order(self, symbol: str, side: str, quantity: float,
                          reduce_only: bool = False) -> dict:
        return self.signed_request("POST", "/fapi/v1/order", {
            "symbol": symbol, "side": side, "type": "MARKET",
            "quantity": quantity, "reduceOnly": "true" if reduce_only else "false",
        })

    def new_algo_stop_market_order(self, symbol: str, side: str, trigger_price: float,
                                    close_position: bool = True, quantity: float | None = None,
                                    order_type: str = "STOP_MARKET",
                                    working_type: str = "CONTRACT_PRICE") -> dict:
        """STOP_MARKET/TAKE_PROFIT_MARKET via the Algo Order API. Response
        key is `algoId`, not `orderId` - cancel/query with
        cancel_algo_order() / get_algo_order(), not the plain-order ones."""
        params = {
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side,
            "type": order_type, "triggerPrice": trigger_price,
            "workingType": working_type,
        }
        if close_position:
            params["closePosition"] = "true"
        else:
            params["quantity"] = quantity
            params["reduceOnly"] = "true"
        return self.signed_request("POST", "/fapi/v1/algoOrder", params)

    def cancel_algo_order(self, symbol: str, algo_id: str) -> dict:
        return self.signed_request("DELETE", "/fapi/v1/algoOrder",
                                    {"symbol": symbol, "algoId": algo_id})

    def get_algo_order(self, symbol: str, algo_id: str) -> dict:
        return self.signed_request("GET", "/fapi/v1/algoOrder",
                                    {"symbol": symbol, "algoId": algo_id})

    def new_limit_order(self, symbol: str, side: str, price: float, quantity: float,
                         reduce_only: bool = True, time_in_force: str = "GTC") -> dict:
        # LIMIT was not part of the 2025-12-09 algo-order migration -
        # still goes through /fapi/v1/order.
        return self.signed_request("POST", "/fapi/v1/order", {
            "symbol": symbol, "side": side, "type": "LIMIT", "price": price,
            "quantity": quantity, "timeInForce": time_in_force,
            "reduceOnly": "true" if reduce_only else "false",
        })

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        return self.signed_request("DELETE", "/fapi/v1/order",
                                    {"symbol": symbol, "orderId": order_id})

    def get_order(self, symbol: str, order_id: str) -> dict:
        return self.signed_request("GET", "/fapi/v1/order",
                                    {"symbol": symbol, "orderId": order_id})
