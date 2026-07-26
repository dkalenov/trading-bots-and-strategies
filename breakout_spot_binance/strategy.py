"""
Breakout Spot Strategy — Strategy module.

Volume-confirmed breakout strategy for Binance Spot.
- BUY: price breaks above lookback-period high with volume spike
- Trailing stop to protect profits
- ATR-based stop loss
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
    lookback: int = 20
    volume_multiplier: float = 2.0
    atr_period: int = 14
    stop_loss_multiplier: float = 1.5
    trailing_stop_pct: float = 3.0
    min_volume_usdt: float = 1_000_000


class BreakoutCore:
    def __init__(self, params: StrategyParams):
        self.params = params
        self.highs = []
        self.lows = []
        self.volumes = []
        self.atr = 0.0
        self.peak_price = 0.0
        self.position_peak = 0.0
        self.initialized = False

    def reset(self):
        self.highs.clear()
        self.lows.clear()
        self.volumes.clear()
        self.atr = 0.0
        self.peak_price = 0.0
        self.position_peak = 0.0
        self.initialized = False

    def update(self, high: float, low: float, close: float,
               volume_usdt: float, atr_value: float) -> Signal:
        self.highs.append(high)
        self.lows.append(low)
        self.volumes.append(volume_usdt)
        self.atr = atr_value

        if len(self.highs) < self.params.lookback + 1:
            return Signal.HOLD

        lookback_highs = self.highs[-(self.params.lookback + 1):-1]
        resistance = max(lookback_highs)

        lookback_volumes = self.volumes[-(self.params.lookback + 1):-1]
        avg_volume = np.mean(lookback_volumes) if lookback_volumes else 0

        volume_spike = volume_usdt >= avg_volume * self.params.volume_multiplier if avg_volume > 0 else False
        min_vol_ok = volume_usdt >= self.params.min_volume_usdt

        if close > resistance and volume_spike and min_vol_ok:
            return Signal.BUY

        return Signal.HOLD

    def open_position(self, entry_price: float):
        self.position_peak = entry_price

    def check_trailing_stop(self, close: float) -> Signal:
        if self.position_peak <= 0:
            return Signal.HOLD
        if close > self.position_peak:
            self.position_peak = close
        if close < self.position_peak * (1 - self.params.trailing_stop_pct / 100):
            self.position_peak = 0.0
            return Signal.SELL
        return Signal.HOLD

    def close_position(self):
        self.position_peak = 0.0


def breakout_signal(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                    volumes: np.ndarray, state: dict, params: StrategyParams) -> str:
    """Array-based breakout signal for backtesting."""
    if len(closes) < params.lookback + params.atr_period + 2:
        return "HOLD"

    atr = calculate_atr(highs, lows, closes, params.atr_period)

    if 'peak_price' not in state:
        state['peak_price'] = 0.0

    i = len(closes) - 1
    close = closes[i]
    high = highs[i]
    low = lows[i]
    volume = volumes[i]
    atr_val = atr[i]

    if np.isnan(atr_val):
        return "HOLD"

    # Lookback high/low (previous lookback bars)
    start = max(0, i - params.lookback)
    resistance = np.max(highs[start:i])
    support = np.min(lows[start:i])

    # Average volume
    avg_vol = np.mean(volumes[start:i]) if i > start else 0
    volume_spike = volume >= avg_vol * params.volume_multiplier if avg_vol > 0 else False
    min_vol_ok = volume >= params.min_volume_usdt

    # Breakout
    if close > resistance and volume_spike and min_vol_ok:
        state['peak_price'] = close
        return "BUY"

    # Trailing stop
    if state['peak_price'] > 0:
        if close < state['peak_price'] * (1 - params.trailing_stop_pct / 100):
            state['peak_price'] = 0.0
            return "SELL"
        if close > state['peak_price']:
            state['peak_price'] = close

    return "HOLD"
