"""
Market-data websocket: Binance's combined kline stream for all tracked
symbols on one connection.

Reconnect handling gets special attention here because a silently-dead
market feed is the single most dangerous failure mode for a trading
bot — you keep "running" while blind to price. This module:
  - pings are handled automatically by the `websockets` library
    (it answers Binance's server-side pings itself); we additionally
    track wall-clock time since the last message and force a
    reconnect if the feed goes quiet for longer than `stale_after`
    seconds, since a network partition can leave a socket looking
    "open" while no data is actually flowing.
  - reconnects with exponential backoff (capped), so a real outage
    doesn't turn into a hot retry loop hammering Binance.
  - never lets one bad message crash the whole feed — a parse error
    is logged and skipped, not raised.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

import websockets

from bars import BarWindow

logger = logging.getLogger(__name__)

OnBarClosed = Callable[[str], Awaitable[None]]


class MarketDataStream:
    def __init__(
        self,
        ws_base_url: str,
        symbols: list[str],
        interval: str,
        bar_windows: dict[str, BarWindow],
        on_bar_closed: OnBarClosed,
        stale_after: float = 180.0,
    ):
        self._ws_base_url = ws_base_url
        self._symbols = symbols
        self._interval = interval
        self._bar_windows = bar_windows
        self._on_bar_closed = on_bar_closed
        self._stale_after = stale_after
        self._stop = asyncio.Event()
        self._last_message_at = time.monotonic()

    def _stream_url(self) -> str:
        streams = "/".join(f"{s.lower()}@kline_{self._interval}" for s in self._symbols)
        return f"{self._ws_base_url}/stream?streams={streams}"

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 1.0  # clean disconnect (e.g. our own stop()) — no penalty
            except Exception as e:
                logger.warning("market stream error: %s — reconnecting in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _run_once(self) -> None:
        url = self._stream_url()
        logger.info("market stream connecting: %s symbols @ %s", len(self._symbols), self._interval)
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            self._last_message_at = time.monotonic()
            watchdog = asyncio.create_task(self._watchdog(ws))
            try:
                async for raw in ws:
                    self._last_message_at = time.monotonic()
                    if self._stop.is_set():
                        break
                    await self._handle_message(raw)
            finally:
                watchdog.cancel()

    async def _watchdog(self, ws) -> None:
        """Force-close the connection if no message arrives for stale_after
        seconds — a hung-but-open socket is worse than a visibly closed one."""
        while True:
            await asyncio.sleep(10)
            if time.monotonic() - self._last_message_at > self._stale_after:
                logger.warning("market stream stale (%.0fs since last message) — forcing reconnect",
                                time.monotonic() - self._last_message_at)
                await ws.close()
                return

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
            payload = msg.get("data", msg)
            k = payload.get("k")
            if not k:
                return
            symbol = payload.get("s") or k.get("s")
            if symbol not in self._bar_windows:
                return
            if not k.get("x"):
                return  # candle still forming — never act on this
            self._bar_windows[symbol].add_closed_bar(
                open_time=k["t"], o=float(k["o"]), h=float(k["h"]),
                l=float(k["l"]), c=float(k["c"]), v=float(k["v"]),
            )
            await self._on_bar_closed(symbol)
        except Exception:
            logger.exception("failed to handle market stream message (skipping): %.200s", raw)
