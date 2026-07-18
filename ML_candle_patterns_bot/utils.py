"""Utility functions — formatting, rounding, sleep, locking."""

import os
import time
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "8h": 28800, "1d": 86400,
}


def fmt_price(price: float, precision: int) -> str:
    """Format price to plain decimal string."""
    return f"{price:.{precision}f}"


def fmt_qty(qty: float, precision: int) -> str:
    """Format quantity to plain decimal string."""
    return f"{qty:.{precision}f}"


def floor_to_step(qty: float, step_size: float) -> float:
    """Round DOWN to nearest step."""
    if step_size <= 0:
        return qty
    factor = 1.0 / step_size
    return int(qty * factor) / factor


def ceil_to_step(qty: float, step_size: float) -> float:
    """Round UP to nearest step."""
    if step_size <= 0:
        return qty
    factor = 1.0 / step_size
    import math
    return math.ceil(qty * factor) / factor


def round_price(price: float, tick_size: float) -> float:
    """Round price to tick_size."""
    if tick_size <= 0:
        return price
    factor = 1.0 / tick_size
    return round(price * factor) / factor


def interval_to_seconds(interval: str) -> int:
    """Convert interval string to seconds."""
    return _INTERVAL_SECONDS.get(interval, 900)


def sleep_to_next_candle(interval: str) -> float:
    """Sleep until next candle close. Returns seconds slept."""
    seconds = interval_to_seconds(interval)
    sleep_time = seconds - time.time() % seconds + 2
    time.sleep(sleep_time)
    return sleep_time


def acquire_lock(lock_path: str) -> bool:
    """Acquire file lock (Windows-safe)."""
    try:
        if os.path.exists(lock_path):
            with open(lock_path) as f:
                pid = int(f.read().strip())
            if pid != os.getpid():
                try:
                    os.kill(pid, 0)
                    logger.error("Another instance running (PID %d)", pid)
                    return False
                except OSError:
                    pass
        with open(lock_path, "w") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        logger.error("Lock failed: %s", e)
        return False


def release_lock(lock_path: str):
    """Release file lock."""
    try:
        if os.path.exists(lock_path):
            with open(lock_path) as f:
                pid = int(f.read().strip())
            if pid == os.getpid():
                os.remove(lock_path)
    except Exception:
        pass


def compute_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    """Compute Average True Range."""
    if len(closes) < 2:
        return 0.0
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def compute_ema(data: list[float], span: int) -> list[float]:
    """Compute Exponential Moving Average."""
    if not data:
        return []
    alpha = 2.0 / (span + 1)
    result = [data[0]]
    for i in range(1, len(data)):
        result.append(alpha * data[i] + (1 - alpha) * result[-1])
    return result


def compute_daily_regime(closes: list[float], interval_seconds: int = 900, ema_long: int = 50) -> str:
    """Compute daily trend regime from bar closes using EMA20/EMA50."""
    bars_per_day = 86400 // interval_seconds
    if bars_per_day < 1:
        bars_per_day = 1
    daily = [closes[i] for i in range(0, len(closes), bars_per_day) if i < len(closes)]
    if len(daily) < ema_long:
        return "unknown"
    ema_s = compute_ema(daily, 20)
    ema_l = compute_ema(daily, ema_long)
    if ema_s[-1] > ema_l[-1] * 1.01:
        return "bull"
    elif ema_s[-1] < ema_l[-1] * 0.99:
        return "bear"
    return "flat"
