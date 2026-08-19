"""
Wilder's ATR. This is the ONLY ATR implementation in the project.
Both the backtester and the live/testnet bot import this function -
there is no second copy anywhere to drift out of sync with this one.
"""
from __future__ import annotations
import pandas as pd
import numpy as np


def wilder_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    df must have columns: High, Low, Close (any index, must be time-sorted).
    Returns a Series aligned to df.index, NaN for the first `period-1` bars.

    First value = simple mean of the first `period` true ranges.
    Every value after that = Wilder smoothing:
        ATR[t] = (ATR[t-1] * (period - 1) + TR[t]) / period
    This matches the standard Pine Script / TradingView ATR, which is the
    ATR the TradingView-based signals in this project are implicitly tied to.
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
    """
    Same formula as wilder_atr(), but one candle at a time, for a future
    websocket-driven version of the bot that doesn't want to refetch
    history every cycle. Not currently called anywhere - bot/trader.py
    fetches the last `atr_length + 5` REST klines each cycle and calls
    wilder_atr() on that, which is simpler and cheap enough at this scale.
    Kept here (and tested) so that "faster incremental ATR" is a five
    minute wire-up later, not a new formula to get right under pressure.
    """
    if prev_close is None or prev_atr is None:
        return None
    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
    return (prev_atr * (period - 1) + tr) / period
