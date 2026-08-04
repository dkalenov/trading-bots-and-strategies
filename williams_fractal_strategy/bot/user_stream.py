"""
User-data websocket: private order/position update events
(ORDER_TRADE_UPDATE, ACCOUNT_UPDATE), via a listenKey.

Binance requires the listenKey to be refreshed with a PUT roughly
every 30-60 minutes or the stream dies silently from the server side.
This module owns that keepalive loop as well as the connection itself,
and transparently gets a fresh listenKey and reconnects on any
disconnect — a caller never has to think about listenKey lifecycle.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import websockets

logger = logging.getLogger(__name__)

OnUserEvent = Callable[[dict[str, Any]], Awaitable[None]]


class UserDataStream:
    def __init__(self, client, ws_base_url: str, on_event: OnUserEvent, keepalive_seconds: int = 1800):
        self._client = client
        self._ws_base_url = ws_base_url
        self._on_event = on_event
        self._keepalive_seconds = keepalive_seconds
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 1.0
            except Exception as e:
                logger.warning("user data stream error: %s — reconnecting in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _run_once(self) -> None:
        listen_key = await self._client.start_listen_key()
        url = f"{self._ws_base_url}/ws/{listen_key}"
        logger.info("user data stream connecting")

        keepalive_task = asyncio.create_task(self._keepalive_loop())
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    await self._handle_message(raw)
        finally:
            keepalive_task.cancel()

    async def _keepalive_loop(self) -> None:
        while True:
            await asyncio.sleep(self._keepalive_seconds)
            try:
                await self._client.keepalive_listen_key()
                logger.debug("listen key keepalive sent")
            except Exception:
                logger.exception("listen key keepalive failed (will get a fresh one on reconnect)")

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
            await self._on_event(msg)
        except Exception:
            logger.exception("failed to handle user data message (skipping): %.200s", raw)
