"""
Bollinger Bands Strategy — Strategy module.

Contains all Bollinger Bands strategy variants and the signal generator.
"""

import numpy as np
from dataclasses import dataclass
from enum import Enum


class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class StrategyParams:
    bb_timeperiod: int = 20
    bb_nbdevup: float = 2.0
    bb_nbdevdn: float = 2.0
    bb_matype: int = 0
    rsi_period: int = 14
    squeeze_atr_period: int = 14
    squeeze_threshold: float = 0.5
    take_profit_multiplier: float = 3.0
    stop_loss_multiplier: float = 1.5
    trailing_stop_pct: float = 2.0


def calculate_bollinger_bands(closes: np.ndarray, timeperiod: int = 20,
                               nbdevup: float = 2.0, nbdevdn: float = 2.0):
    """Calculate Bollinger Bands using SMA and standard deviation."""
    n = len(closes)
    middle = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)

    for i in range(timeperiod - 1, n):
        window = closes[i - timeperiod + 1:i + 1]
        sma = np.mean(window)
        std = np.std(window, ddof=0)
        middle[i] = sma
        upper[i] = sma + nbdevup * std
        lower[i] = sma - nbdevdn * std

    return upper, middle, lower


def calculate_rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
    """Calculate RSI using Wilder's exponential smoothing."""
    n = len(closes)
    rsi = np.full(n, 50.0)
    if n < period + 1:
        return rsi

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    # First RSI value at bar `period`
    if avg_loss == 0:
        rsi[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100 - (100 / (1 + rs))

    for i in range(period, n - 1):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i + 1] = 100 - (100 / (1 + rs))

    return rsi


def calculate_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                  period: int = 14) -> np.ndarray:
    """Calculate Average True Range using Wilder's smoothing."""
    n = len(closes)
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1])
        )
    )
    atr = np.full(n, np.nan)
    if len(tr) >= period:
        atr[period] = np.mean(tr[:period])
        for i in range(period + 1, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i - 1]) / period
    return atr


class BollingerBandsCore:
    """Basic Bollinger Bands crossover strategy."""

    def __init__(self, params: StrategyParams):
        self.params = params
        self.prev_close = 0.0
        self.prev_upper = np.nan
        self.prev_lower = np.nan
        self.initialized = False

    def reset(self):
        self.prev_close = 0.0
        self.prev_upper = np.nan
        self.prev_lower = np.nan
        self.initialized = False

    def update(self, close: float, upper: float, middle: float, lower: float) -> Signal:
        if np.isnan(upper) or np.isnan(lower):
            self.prev_close = close
            self.prev_upper = upper
            self.prev_lower = lower
            self.initialized = True
            return Signal.HOLD

        signal = Signal.HOLD

        if self.initialized:
            # BUY: close crosses above upper band
            if close > upper and self.prev_close <= self.prev_upper:
                signal = Signal.BUY
            # SELL: close crosses below lower band
            elif close < lower and self.prev_close >= self.prev_lower:
                signal = Signal.SELL

        self.prev_close = close
        self.prev_upper = upper
        self.prev_lower = lower
        self.initialized = True

        return signal


class BollingerRSIFilter:
    """Bollinger Bands + RSI filter strategy."""

    def __init__(self, params: StrategyParams):
        self.params = params
        self.prev_close = 0.0
        self.prev_upper = np.nan
        self.prev_lower = np.nan
        self.initialized = False

    def reset(self):
        self.prev_close = 0.0
        self.prev_upper = np.nan
        self.prev_lower = np.nan
        self.initialized = False

    def update(self, close: float, upper: float, middle: float, lower: float,
               rsi: float) -> Signal:
        if np.isnan(upper) or np.isnan(lower):
            self.prev_close = close
            self.prev_upper = upper
            self.prev_lower = lower
            self.initialized = True
            return Signal.HOLD

        signal = Signal.HOLD

        if self.initialized:
            # BUY: cross above upper + RSI > 55 (momentum confirmation)
            if close > upper and self.prev_close <= self.prev_upper and rsi > 55:
                signal = Signal.BUY
            # SELL: cross below lower + RSI < 45
            elif close < lower and self.prev_close >= self.prev_lower and rsi < 45:
                signal = Signal.SELL

        self.prev_close = close
        self.prev_upper = upper
        self.prev_lower = lower
        self.initialized = True

        return signal


class BollingerSqueezeFilter:
    """Bollinger Bands squeeze breakout strategy."""

    def __init__(self, params: StrategyParams):
        self.params = params
        self.prev_close = 0.0
        self.prev_upper = np.nan
        self.prev_lower = np.nan
        self.prev_middle = np.nan
        self.squeeze_active = False
        self.initialized = False

    def reset(self):
        self.prev_close = 0.0
        self.prev_upper = np.nan
        self.prev_lower = np.nan
        self.prev_middle = np.nan
        self.squeeze_active = False
        self.initialized = False

    def update(self, close: float, upper: float, middle: float, lower: float,
               bb_widths: np.ndarray, bar_idx: int) -> Signal:
        if np.isnan(upper) or np.isnan(lower) or np.isnan(middle):
            self.prev_close = close
            self.prev_upper = upper
            self.prev_lower = lower
            self.prev_middle = middle
            self.initialized = True
            return Signal.HOLD

        signal = Signal.HOLD

        if self.initialized and not np.isnan(bb_widths[bar_idx]):
            current_width = bb_widths[bar_idx]
            # Compute average width over last 50 bars
            lookback = min(50, bar_idx)
            if lookback > 0:
                avg_width = np.nanmean(bb_widths[max(0, bar_idx - lookback):bar_idx])
                is_squeeze = current_width < self.params.squeeze_threshold * avg_width

                if is_squeeze:
                    self.squeeze_active = True
                elif self.squeeze_active:
                    # Squeeze breakout
                    self.squeeze_active = False
                    if close > upper:
                        signal = Signal.BUY
                    elif close < lower:
                        signal = Signal.SELL

        self.prev_close = close
        self.prev_upper = upper
        self.prev_lower = lower
        self.prev_middle = middle
        self.initialized = True

        return signal


class BollingerMeanReversion:
    """Bollinger Bands mean reversion strategy."""

    def __init__(self, params: StrategyParams):
        self.params = params
        self.prev_close = 0.0
        self.position_side = 0  # 0=none, 1=long, -1=short
        self.initialized = False

    def reset(self):
        self.prev_close = 0.0
        self.position_side = 0
        self.initialized = False

    def update(self, close: float, upper: float, middle: float, lower: float) -> Signal:
        if np.isnan(upper) or np.isnan(lower) or np.isnan(middle):
            self.prev_close = close
            self.initialized = True
            return Signal.HOLD

        signal = Signal.HOLD

        if self.initialized:
            if self.position_side == 0:
                # No position: enter on band touch
                if close <= lower and self.prev_close > lower:
                    signal = Signal.BUY
                    self.position_side = 1
                elif close >= upper and self.prev_close < upper:
                    signal = Signal.SELL
                    self.position_side = -1
            elif self.position_side == 1:
                # Long: exit at middle band or opposite band
                if close >= middle:
                    signal = Signal.SELL
                    self.position_side = 0
                elif close < lower:
                    signal = Signal.SELL
                    self.position_side = 0
            elif self.position_side == -1:
                # Short: exit at middle band or opposite band
                if close <= middle:
                    signal = Signal.BUY
                    self.position_side = 0
                elif close > upper:
                    signal = Signal.BUY
                    self.position_side = 0

        self.prev_close = close
        self.initialized = True

        return signal
