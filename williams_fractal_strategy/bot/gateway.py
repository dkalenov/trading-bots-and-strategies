"""
Binance USDT-M Futures REST client — signed requests over aiohttp.

Only the endpoints this bot actually needs. Every write call
(new_order, cancel_order, change_leverage, listen key management) is a
real network call — nothing here is simulated; see dry_run_gateway.py
for the no-network alternative used by EXECUTION_MODE=dry_run.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
import urllib.parse
from decimal import Decimal
from typing import Any

import aiohttp

from filters import SymbolFilters

logger = logging.getLogger(__name__)


class BinanceAPIError(Exception):
    def __init__(self, status: int, code: int | None, msg: str, payload: dict | None = None):
        self.status = status
        self.code = code
        self.msg = msg
        self.payload = payload or {}
        super().__init__(f"HTTP {status} code={code}: {msg}")


class Gateway:
    """Thin, explicit wrapper — every method maps to exactly one Binance endpoint."""

    def __init__(self, api_key: str, api_secret: str, base_url: str, recv_window: int = 5000):
        self._api_key = api_key
        self._api_secret = api_secret.encode()
        self._base_url = base_url.rstrip("/")
        self._recv_window = recv_window
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Gateway":
        self._session = aiohttp.ClientSession(
            headers={"X-MBX-APIKEY": self._api_key},
            timeout=aiohttp.ClientTimeout(total=15),
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    def _sign(self, params: dict[str, Any]) -> dict[str, Any]:
        params = {**params, "timestamp": int(time.time() * 1000), "recvWindow": self._recv_window}
        query = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(self._api_secret, query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    async def _request(
        self, method: str, path: str, *, signed: bool = False, params: dict | None = None,
    ) -> Any:
        assert self._session is not None, "use 'async with Gateway(...)'"
        params = dict(params or {})
        if signed:
            params = self._sign(params)

        url = f"{self._base_url}{path}"
        async with self._session.request(method, url, params=params) as resp:
            text = await resp.text()
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = {"raw": text}
            if resp.status >= 400:
                raise BinanceAPIError(
                    resp.status, data.get("code") if isinstance(data, dict) else None,
                    data.get("msg", text) if isinstance(data, dict) else text, data if isinstance(data, dict) else {},
                )
            return data

    # ---- public/market ----

    async def exchange_info(self) -> dict:
        return await self._request("GET", "/fapi/v1/exchangeInfo")

    async def get_symbol_filters(self, symbol: str) -> SymbolFilters:
        info = await self.exchange_info()
        for sym in info["symbols"]:
            if sym["symbol"] == symbol:
                return SymbolFilters.from_exchange_info_symbol(sym)
        raise ValueError(f"{symbol} not found in exchangeInfo")

    async def klines(self, symbol: str, interval: str, limit: int = 200) -> list[list]:
        return await self._request(
            "GET", "/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit}
        )

    # ---- account ----

    async def account_balance(self) -> list[dict]:
        return await self._request("GET", "/fapi/v2/balance", signed=True)

    async def usdt_equity(self) -> Decimal:
        balances = await self.account_balance()
        for b in balances:
            if b.get("asset") == "USDT":
                return Decimal(str(b.get("balance", "0")))
        return Decimal("0")

    async def change_leverage(self, symbol: str, leverage: int) -> dict:
        return await self._request(
            "POST", "/fapi/v1/leverage", signed=True, params={"symbol": symbol, "leverage": leverage}
        )

    async def position_risk(self, symbol: str) -> list[dict]:
        return await self._request("GET", "/fapi/v2/positionRisk", signed=True, params={"symbol": symbol})

    # ---- orders ----

    async def new_order(self, **params) -> dict:
        return await self._request("POST", "/fapi/v1/order", signed=True, params=params)

    async def get_order(self, symbol: str, order_id: int) -> dict:
        return await self._request(
            "GET", "/fapi/v1/order", signed=True, params={"symbol": symbol, "orderId": order_id}
        )

    async def wait_for_order_fill(self, symbol: str, order_id: int, timeout: float = 30.0) -> dict:
        import asyncio
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                o = await self.get_order(symbol, order_id)
                if o.get("status") == "FILLED":
                    return o
                if o.get("status") in ("CANCELED", "EXPIRED", "REJECTED"):
                    raise BinanceAPIError(400, None, f"order {order_id} {o.get('status')}")
            except Exception:
                raise
            await asyncio.sleep(0.5)
        raise BinanceAPIError(408, None, f"order {order_id} fill timeout after {timeout}s")

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        return await self._request(
            "DELETE", "/fapi/v1/order", signed=True, params={"symbol": symbol, "orderId": order_id}
        )

    async def cancel_all_open_orders(self, symbol: str) -> dict:
        return await self._request(
            "DELETE", "/fapi/v1/allOpenOrders", signed=True, params={"symbol": symbol}
        )

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        params = {"symbol": symbol} if symbol else {}
        return await self._request("GET", "/fapi/v1/openOrders", signed=True, params=params)

    async def all_positions(self) -> list[dict]:
        return await self._request("GET", "/fapi/v2/positionRisk", signed=True)

    # ---- algo orders (Binance migrated STOP_MARKET / TAKE_PROFIT_MARKET
    #      to /fapi/v1/algoOrder on 2025-12-09) ----

    async def new_algo_order(self, **params) -> dict:
        return await self._request("POST", "/fapi/v1/algoOrder", signed=True, params=params)

    async def open_algo_orders(self, symbol: str | None = None) -> list[dict]:
        params = {"symbol": symbol} if symbol else {}
        data = await self._request("GET", "/fapi/v1/openAlgoOrders", signed=True, params=params)
        return data.get("orders", []) if isinstance(data, dict) else (data or [])

    async def cancel_algo_order(self, symbol: str, algo_id: int) -> dict:
        return await self._request(
            "DELETE", "/fapi/v1/algoOrder", signed=True,
            params={"symbol": symbol, "algoId": algo_id},
        )

    # ---- user data stream ----

    async def start_listen_key(self) -> str:
        data = await self._request("POST", "/fapi/v1/listenKey", signed=True)
        return data["listenKey"]

    async def keepalive_listen_key(self) -> None:
        await self._request("PUT", "/fapi/v1/listenKey", signed=True)

    async def close_listen_key(self) -> None:
        await self._request("DELETE", "/fapi/v1/listenKey", signed=True)
