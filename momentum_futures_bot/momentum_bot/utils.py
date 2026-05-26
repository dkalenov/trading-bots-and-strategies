"""Utility functions for the momentum bot."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def round_price(price: float, tick_size: int) -> float:
    """Round price to tick_size decimal places."""
    return round(price, tick_size)


def klines_to_dataframe(klines: list[list]) -> pd.DataFrame:
    """Convert Binance REST klines to DataFrame matching momentum_core format.

    Binance klines format: [open_time, open, high, low, close, volume, close_time, ...]
    Output: DatetimeIndex, columns: open, high, low, close, volume
    """
    if not klines:
        return pd.DataFrame()

    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df.index = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["open", "high", "low", "close", "volume"]]
    return df


async def wait_for_next_candle(timeframe: str = "4h", offset_seconds: int = 5,
                                shutdown_event: asyncio.Event = None):
    """Sleep until next candle close + offset.
    Supports: 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d.
    Returns early if shutdown_event is set.
    """
    intervals = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "8h": 28800, "1d": 86400,
    }
    interval_sec = intervals.get(timeframe, 14400)

    now = datetime.now(timezone.utc)
    epoch = now.timestamp()
    next_close = (int(epoch / interval_sec) + 1) * interval_sec + offset_seconds
    wait = max(0, next_close - epoch)

    next_time = datetime.fromtimestamp(next_close, tz=timezone.utc)
    logger.info(f"⏰ Next {timeframe} candle: {next_time.strftime('%H:%M:%S UTC')} (in {wait/60:.1f}m)")

    if shutdown_event:
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=wait)
        except asyncio.TimeoutError:
            pass  # Normal: candle time reached
    else:
        await asyncio.sleep(wait)
