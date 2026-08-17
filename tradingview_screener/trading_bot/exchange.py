"""
Minimal, transparent Binance USDM Futures REST client. Deliberately not
using python-binance's Futures wrapper - a plain requests + hmac client
is about 150 lines, fully auditable, and won't silently break when a
third-party wrapper changes its method signatures between versions.

Covers exactly what the bot needs: klines, mark price, exchange filters,
placing/cancelling orders, reading position/order status. Nothing else.

Endpoint paths are current as of this writing - Binance does version its
futures API (v1/v2/v3 for some endpoints), so if an endpoint starts
returning 404, check https://binance-docs.github.io/apidocs/futures/en/
before assuming the bot logic is wrong.
"""
from __future__ import annotations
import hashlib
import hmac
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

TESTNET_URL = "https://testnet.binancefuture.com"
MAINNET_URL = "https://fapi.binance.com"


class BinanceAPIError(RuntimeError):
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self.payload = payload
        super().__init__(f"Binance API error {status_code}: {payload}")


@dataclass
class SymbolFilters:
    step_size: float
    tick_size: float
    min_notional: float


class BinanceFuturesClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True,
                 recv_window: int = 5000, timeout: int = 10):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.base_url = TESTNET_URL if testnet else MAINNET_URL
        self.recv_window = recv_window
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": api_key})

    def _sign(self, params: dict) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(self.api_secret, query.encode(), hashlib.sha256).hexdigest()

    def _signed_request(self, method: str, path: str, params: dict | None = None) -> dict:
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self.recv_window
        params["signature"] = self._sign(params)
        resp = self.session.request(method, self.base_url + path, params=params, timeout=self.timeout)
        return self._handle(resp)

    def _public_request(self, method: str, path: str, params: dict | None = None) -> dict:
        resp = self.session.request(method, self.base_url + path, params=params or {}, timeout=self.timeout)
        return self._handle(resp)

    @staticmethod
    def _handle(resp: requests.Response):
        try:
            data = resp.json()
        except ValueError:
            resp.raise_for_status()
            raise
        if resp.status_code >= 400:
            raise BinanceAPIError(resp.status_code, data)
        return data

    # ---- public/market data ----

    def get_klines(self, symbol: str, interval: str = "4h", limit: int = 200) -> list:
        return self._public_request("GET", "/fapi/v1/klines",
                                     {"symbol": symbol, "interval": interval, "limit": limit})

    def get_mark_price(self, symbol: str) -> float:
        data = self._public_request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data["markPrice"])

    def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        info = self._public_request("GET", "/fapi/v1/exchangeInfo")
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                step = tick = min_notional = None
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                    elif f["filterType"] == "PRICE_FILTER":
                        tick = float(f["tickSize"])
                    elif f["filterType"] == "MIN_NOTIONAL":
                        min_notional = float(f.get("notional", f.get("minNotional", 5.0)))
                return SymbolFilters(step_size=step or 0.001, tick_size=tick or 0.01,
                                      min_notional=min_notional or 5.0)
        raise ValueError(f"symbol {symbol} not found in exchangeInfo")

    # ---- account / trading (signed) ----

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return self._signed_request("POST", "/fapi/v1/leverage",
                                     {"symbol": symbol, "leverage": leverage})

    def get_position(self, symbol: str) -> dict | None:
        rows = self._signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
        for r in rows:
            if r["symbol"] == symbol and float(r["positionAmt"]) != 0:
                return r
        return None

    def new_market_order(self, symbol: str, side: str, quantity: float,
                          reduce_only: bool = False) -> dict:
        return self._signed_request("POST", "/fapi/v1/order", {
            "symbol": symbol, "side": side, "type": "MARKET",
            "quantity": quantity, "reduceOnly": "true" if reduce_only else "false",
        })

    def new_stop_market_order(self, symbol: str, side: str, stop_price: float,
                               close_position: bool = True, quantity: float | None = None) -> dict:
        if "testnet" in self.base_url:
            params = {
                "symbol": symbol, "side": side, "type": "STOP_MARKET",
                "triggerprice": stop_price, "closePosition": "true",
                "workingType": "CONTRACT_PRICE", "timeInForce": "GTE_GTC",
                "algotype": "CONDITIONAL",
            }
            return self._signed_request("POST", "/fapi/v1/algoOrder", params)
        params = {"symbol": symbol, "side": side, "type": "STOP_MARKET",
                   "stopPrice": stop_price}
        if close_position:
            params["closePosition"] = "true"
        else:
            params["quantity"] = quantity
            params["reduceOnly"] = "true"
        return self._signed_request("POST", "/fapi/v1/order", params)

    def new_limit_order(self, symbol: str, side: str, price: float, quantity: float,
                         reduce_only: bool = True, time_in_force: str = "GTC") -> dict:
        return self._signed_request("POST", "/fapi/v1/order", {
            "symbol": symbol, "side": side, "type": "LIMIT", "price": price,
            "quantity": quantity, "timeInForce": time_in_force,
            "reduceOnly": "true" if reduce_only else "false",
        })

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        return self._signed_request("DELETE", "/fapi/v1/order",
                                     {"symbol": symbol, "orderId": order_id})

    def get_order(self, symbol: str, order_id: str) -> dict:
        return self._signed_request("GET", "/fapi/v1/order",
                                     {"symbol": symbol, "orderId": order_id})
