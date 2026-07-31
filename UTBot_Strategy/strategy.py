"""
UTBot Strategy — Strategy module.

Contains all UTBot strategy variants and the signal generator.
"""

import numpy as np
from dataclasses import dataclass
from enum import Enum

from utils import calculate_atr


class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class StrategyParams:
    key_value: float = 8.0
    atr_period: int = 10
    take_profit_multiplier: float = 3.0
    stop_loss_multiplier: float = 1.5
    rsi_period: int = 14
    supertrend_atr_period: int = 10
    supertrend_multiplier: float = 1.0
    trailing_stop_pct: float = 2.0


class UTBotCore:
    def __init__(self, params: StrategyParams):
        self.params = params
        self.x_atr_trailing_stop = 0.0
        self.pos = 0
        self.ema = 0.0
        self.prev_close = 0.0
        self.initialized = False

    def reset(self):
        self.x_atr_trailing_stop = 0.0
        self.pos = 0
        self.ema = 0.0
        self.prev_close = 0.0
        self.initialized = False

    def update(self, close: float, atr_value: float) -> Signal:
        if atr_value <= 0 or np.isnan(atr_value):
            self.prev_close = close
            return Signal.HOLD

        n_loss = atr_value * self.params.key_value

        if not self.initialized:
            self.x_atr_trailing_stop = close
            self.prev_close = close
            self.ema = close
            self.pos = 0
            self.initialized = True
            return Signal.HOLD

        # ATR trailing stop calculation
        if close > self.x_atr_trailing_stop:
            iff_1 = close - n_loss
        else:
            iff_1 = close + n_loss

        if close < self.x_atr_trailing_stop and self.prev_close < self.x_atr_trailing_stop:
            iff_2 = min(self.x_atr_trailing_stop, close + n_loss)
        else:
            iff_2 = iff_1

        if close > self.x_atr_trailing_stop and self.prev_close > self.x_atr_trailing_stop:
            new_trailing_stop = max(self.x_atr_trailing_stop, close - n_loss)
        else:
            new_trailing_stop = iff_2

        # Position tracking
        if self.prev_close > self.x_atr_trailing_stop and close < self.x_atr_trailing_stop:
            new_pos = -1
        elif self.prev_close < self.x_atr_trailing_stop and close > self.x_atr_trailing_stop:
            new_pos = 1
        else:
            new_pos = self.pos

        ema = close
        above = ema > new_trailing_stop and self.ema <= self.x_atr_trailing_stop
        below = new_trailing_stop > ema and self.x_atr_trailing_stop <= self.ema

        buy = close > new_trailing_stop and above
        sell = close < new_trailing_stop and below

        self.x_atr_trailing_stop = new_trailing_stop
        self.pos = new_pos
        self.ema = ema
        self.prev_close = close

        if buy:
            return Signal.BUY
        elif sell:
            return Signal.SELL
        return Signal.HOLD


class SuperTrendFilter:
    def __init__(self, atr_period: int = 10, multiplier: float = 3.0):
        self.atr_period = atr_period
        self.multiplier = multiplier
        self.up = 0.0
        self.down = 0.0
        self.trend = 1
        self.prev_up = 0.0
        self.prev_down = 0.0
        self.initialized = False
        self.super_trend_signal = 1

    def reset(self):
        self.up = 0.0
        self.down = 0.0
        self.trend = 1
        self.prev_up = 0.0
        self.prev_down = 0.0
        self.initialized = False
        self.super_trend_signal = 1

    def update(self, high: float, low: float, close: float, atr_value: float) -> int:
        if atr_value <= 0 or np.isnan(atr_value):
            return self.super_trend_signal

        hl2 = (high + low) / 2

        # TradingView convention: upperBand = hl2 + mult*ATR, lowerBand = hl2 - mult*ATR
        up_basic = hl2 + self.multiplier * atr_value
        down_basic = hl2 - self.multiplier * atr_value

        if not self.initialized:
            self.up = up_basic
            self.down = down_basic
            self.prev_up = up_basic
            self.prev_down = down_basic
            self.trend = 1
            self.super_trend_signal = 1
            self.initialized = True
            return self.super_trend_signal

        # Smoothed bands (ratchet toward price)
        if close < self.prev_up:
            self.up = min(self.prev_up, up_basic)
        else:
            self.up = up_basic

        if close > self.prev_down:
            self.down = max(self.prev_down, down_basic)
        else:
            self.down = down_basic

        # Trend change: use CURRENT smoothed values
        if self.trend == -1 and close > self.up:
            self.trend = 1
        elif self.trend == 1 and close < self.down:
            self.trend = -1

        # Signal follows trend
        if self.trend == 1:
            self.super_trend_signal = 1
        else:
            self.super_trend_signal = -1

        self.prev_up = self.up
        self.prev_down = self.down
        return self.super_trend_signal


class RSIFilter:
    def __init__(self, period: int = 14):
        self.period = period
        self.prices = []
        self.rsi = 50.0
        self.avg_gain = 0.0
        self.avg_loss = 0.0
        self.initialized = False

    def reset(self):
        self.prices = []
        self.rsi = 50.0
        self.avg_gain = 0.0
        self.avg_loss = 0.0
        self.initialized = False

    def update(self, close: float) -> float:
        self.prices.append(close)
        if len(self.prices) < self.period + 1:
            return self.rsi

        deltas = np.diff(self.prices[-(self.period + 1):])
        gain = max(deltas[-1], 0)
        loss = max(-deltas[-1], 0)

        if not self.initialized:
            self.avg_gain = np.mean(np.where(deltas > 0, deltas, 0))
            self.avg_loss = np.mean(np.where(deltas < 0, -deltas, 0))
            self.initialized = True
        else:
            self.avg_gain = (self.avg_gain * (self.period - 1) + gain) / self.period
            self.avg_loss = (self.avg_loss * (self.period - 1) + loss) / self.period

        if self.avg_loss == 0:
            self.rsi = 100.0
        else:
            rs = self.avg_gain / self.avg_loss
            self.rsi = 100 - (100 / (1 + rs))

        return self.rsi


def utbot_signal(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                 state: dict, key_value: float = 8, atr_period: int = 10) -> str:
    if len(closes) < atr_period + 2:
        return "HOLD"

    atr = calculate_atr(highs, lows, closes, atr_period)
    close = closes[-1]
    prev_close = closes[-2]
    n_loss = atr[-1] * key_value

    if 'x_atr_trailing_stop' not in state:
        state['x_atr_trailing_stop'] = [0.0]
        state['pos'] = [0]
        state['ema'] = [0.0]

    x_prev = state['x_atr_trailing_stop'][-1]

    if close > x_prev:
        iff_1 = close - n_loss
    else:
        iff_1 = close + n_loss

    if close < x_prev and prev_close < x_prev:
        iff_2 = min(x_prev, close + n_loss)
    else:
        iff_2 = iff_1

    if close > x_prev and prev_close > x_prev:
        x_new = max(x_prev, close - n_loss)
    else:
        x_new = iff_2

    iff_3 = -1 if prev_close > x_prev and close < x_prev else state['pos'][-1]
    pos = 1 if prev_close < x_prev and close > x_prev else iff_3

    ema = close
    above = ema > x_new and state['ema'][-1] <= x_prev
    below = x_new > ema and x_prev <= state['ema'][-1]

    buy = close > x_new and above
    sell = close < x_new and below

    state['x_atr_trailing_stop'].append(x_new)
    state['pos'].append(pos)
    state['ema'].append(ema)

    if buy:
        return "BUY"
    elif sell:
        return "SELL"
    return "HOLD"

