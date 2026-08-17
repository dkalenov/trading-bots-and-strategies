"""
Utility functions used across the project - ATR, Decimal-safe rounding,
Binance kline conversion, candle-close timing. This is the one place any
of these exist; backtest/, execution/, and strategies/ all import from
here rather than keeping their own copies.

Rounding uses Decimal, never plain float division - see quantize_down()
below for why: float arithmetic on step sizes routinely produces values
like 0.0009000000000000001, which Binance rejects with -1111 "Precision
is over the maximum defined for this asset" (see docs/AUDIT.md, T1 - a
real bug this exact file exists to prevent a second occurrence of).
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, Decimal

import numpy as np
import pandas as pd

INTERVAL_SECONDS: dict[str, int] = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "8h": 28800, "1d": 86400,
}


# === Wilder ATR ===============================================================

def wilder_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    df must have columns High, Low, Close, time-sorted. NaN for the first
    `period - 1` bars. First real value = simple mean of the first
    `period` true ranges; every value after that is Wilder smoothing:
        ATR[t] = (ATR[t-1] * (period - 1) + TR[t]) / period
    This is the only ATR implementation in the project.
    """
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    atr = pd.Series(index=df.index, dtype=float)
    if len(tr) < period:
        atr[:] = np.nan
        return atr

    atr.iloc[:period - 1] = np.nan
    atr.iloc[period - 1] = tr.iloc[:period].mean()
    for i in range(period, len(tr)):
        atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr.iloc[i]) / period
    return atr


def wilder_atr_incremental(prev_atr: float | None, high: float, low: float,
                            prev_close: float | None, period: int = 14) -> float | None:
    """Same formula as wilder_atr(), one candle at a time. Not currently
    called anywhere - execution/position_manager.py fetches the last
    `atr_length + 5` REST klines each cycle and calls wilder_atr() on
    that, which is simpler and cheap enough at this scale. Kept here
    (and tested) so a future websocket-driven version doesn't need a
    new formula, just a wire-up."""
    if prev_close is None or prev_atr is None:
        return None
    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
    return (prev_atr * (period - 1) + tr) / period


# === Decimal-safe step/tick rounding ==========================================

def _step_to_decimal(step: float) -> Decimal:
    # str(step), not Decimal(step) directly - Decimal(0.001) is
    # 0.001000000000000000020816... via raw float binary representation;
    # str(0.001) round-trips to the clean "0.001" Binance actually sent.
    return Decimal(str(step))


def quantize_down(value: float, step: float) -> float:
    """Round down to a multiple of step. Used for order quantity - a
    quantity that rounds up past what the account can afford or that
    oversizes the position is worse than one that's a touch small, so
    quantity always rounds down, never to nearest, never up."""
    if step <= 0:
        return value
    step_dec = _step_to_decimal(step)
    value_dec = Decimal(str(value))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * step_dec)


def quantize_up(value: float, step: float) -> float:
    """Round up to a multiple of step. Used when bumping a too-small
    order up to the exchange's minimum notional."""
    if step <= 0:
        return value
    step_dec = _step_to_decimal(step)
    value_dec = Decimal(str(value))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_UP)
    return float(steps * step_dec)


def quantize_price(value: float, tick_size: float) -> float:
    """Round to the nearest multiple of tick_size. Used for stop
    triggerPrice and take-profit limit price - unlike quantity, nearest
    is correct here, Binance just needs price % tickSize == 0, there's
    no "must not exceed" direction to respect (see docs/AUDIT.md, T3)."""
    if tick_size <= 0:
        return value
    step_dec = _step_to_decimal(tick_size)
    value_dec = Decimal(str(value))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_HALF_UP)
    return float(steps * step_dec)


def fmt_price(price: float, tick_size: float) -> str:
    """Format a price as a plain decimal string (never scientific
    notation) at the precision tick_size implies - for building request
    params, where Binance expects e.g. "63289.2", not "63289.19999999"."""
    decimals = max(0, -_step_to_decimal(tick_size).as_tuple().exponent)
    return f"{quantize_price(price, tick_size):.{decimals}f}"


def fmt_qty(qty: float, step_size: float) -> str:
    """Same as fmt_price() but for order quantity / step_size."""
    decimals = max(0, -_step_to_decimal(step_size).as_tuple().exponent)
    return f"{quantize_down(qty, step_size):.{decimals}f}"


# === Klines =====================================================================

def klines_to_dataframe(raw_klines: list) -> pd.DataFrame:
    """Binance /fapi/v1/klines REST response -> DataFrame with
    Open/High/Low/Close/Volume as float columns. The one converter in
    the project - execution/position_manager.py and backtest/engine.py
    both produce/consume the same shape."""
    df = pd.DataFrame(raw_klines, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume", "CloseTime",
        "QuoteVolume", "Trades", "TakerBuyBase", "TakerBuyQuote", "Ignore",
    ])
    for c in ("Open", "High", "Low", "Close", "Volume"):
        df[c] = df[c].astype(float)
    return df


# === Candle timing ===============================================================

def candle_open_time(now: datetime, interval: str) -> datetime:
    """Floor `now` to the start of the current candle for `interval`."""
    interval_seconds = INTERVAL_SECONDS[interval]
    epoch = int(now.timestamp())
    floored = epoch - (epoch % interval_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)
