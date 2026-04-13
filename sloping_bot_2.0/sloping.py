from collections import deque
from dataclasses import dataclass

import numpy as np


@dataclass
class Signal:
    side: bool
    price: float
    atr: float
    line: tuple[float, float]


class Sloping:
    length: int
    ts: deque
    open: deque
    high: deque
    low: deque
    close: deque
    body_up: deque
    body_down: deque

    def __init__(
        self,
        length,
        atr_length=14,
        min_space=3,
        max_len=500,
        debug=False,
        # --- Параметры фильтров ---
        min_touches=2,          # минимум касаний линии для валидности
        breakout_buffer=0.1,    # ATR-буфер на пробой (доля ATR)
        slope_filter=True,      # фильтр по наклону линии
        use_trend_filter=True,  # SMA тренд-фильтр
        trend_sma=200,          # длина SMA для тренд-фильтра
        vol_filter=False,       # фильтр волатильности (ATR растёт)
        vol_lookback=5,         # на сколько баров назад сравнивать ATR
    ):
        self.length = int(length)
        self.atr_length = int(atr_length)
        self.min_space = int(min_space)
        self.max_len = int(max_len)
        self.debug = debug

        # Параметры фильтров
        self.min_touches = int(min_touches)
        self.breakout_buffer = float(breakout_buffer)
        self.slope_filter = bool(slope_filter)
        self.use_trend_filter = bool(use_trend_filter)
        self.trend_sma = int(trend_sma)
        self.vol_filter = bool(vol_filter)
        self.vol_lookback = int(vol_lookback)

        self._last_signal_resistance = self.min_space + 1
        self._last_signal_support = self.min_space + 1

        for key, value in type(self).__annotations__.items():
            if issubclass(value, deque):
                setattr(self, key, deque(maxlen=self.max_len))

    def add_kline(self, ts, open, high, low, close):
        ts = int(ts)
        if not self.ts or ts > self.ts[-1]:
            open_price = float(open)
            close_price = float(close)
            self.ts.append(ts)
            self.open.append(open_price)
            self.high.append(float(high))
            self.low.append(float(low))
            self.close.append(close_price)
            self.body_up.append(max(open_price, close_price))
            self.body_down.append(min(open_price, close_price))
            return True
        return False

    def get_value(self, support=True, resistance=True):
        if len(self.ts) < self.length + 1:
            return None

        # Предвычисляем ATR один раз — используется в нескольких фильтрах
        atr = self.get_atr()
        if not np.isfinite(atr) or atr <= 0:
            return None

        # Volatility filter: торгуем только когда ATR растёт (волатильность расширяется)
        if self.vol_filter:
            atr_prev = self.get_atr_at(-self.vol_lookback)
            if atr_prev is not None and atr <= atr_prev:
                return None

        # SMA тренд-фильтр: определяем направление тренда
        trend_long_ok = True
        trend_short_ok = True
        if self.use_trend_filter and len(self.close) >= self.trend_sma:
            sma = np.mean(list(self.close)[-self.trend_sma:])
            trend_long_ok = self.close[-1] > sma
            trend_short_ok = self.close[-1] < sma

        for is_resistance, enabled in ((False, support), (True, resistance)):
            if not enabled:
                continue

            # Тренд-фильтр: long только выше SMA, short только ниже SMA
            if is_resistance and not trend_long_ok:
                continue
            if not is_resistance and not trend_short_ok:
                continue

            source = self.body_up if is_resistance else self.body_down
            window = self._get_history_window(source)
            if window is None:
                continue

            x = np.arange(len(window), dtype=float)
            slope, intercept = np.polyfit(x, window, 1)
            points = slope * x + intercept
            value_index = (window - points).argmax() if is_resistance else (window - points).argmin()
            line = self._get_line(window, slope, value_index, is_resistance)

            # --- Фильтр наклона ---
            if self.slope_filter:
                if is_resistance and line[0] > 0:
                    # Пробой восходящей resistance — слабый сигнал, пропускаем
                    continue
                if not is_resistance and line[0] < 0:
                    # Пробой нисходящей support — слабый сигнал, пропускаем
                    continue

            # --- Фильтр касаний ---
            touches = self._count_touches(window, line, atr)
            if touches < self.min_touches:
                continue

            # --- Проверка пробоя с ATR-буфером ---
            breakout_level = self._line_price(line, len(window))
            buffer = self.breakout_buffer * atr

            has_breakout = self.debug or (
                self.close[-1] > breakout_level + buffer if is_resistance
                else self.close[-1] < breakout_level - buffer
            )

            counter_name = "_last_signal_resistance" if is_resistance else "_last_signal_support"
            counter_value = getattr(self, counter_name)

            if has_breakout:
                setattr(self, counter_name, 0)
                if counter_value > self.min_space:
                    return Signal(is_resistance, self.close[-1], float(atr), line)
            else:
                setattr(self, counter_name, counter_value + 1)

        return None

    def _get_history_window(self, values):
        if len(values) < self.length + 1:
            return None
        return np.array(list(values)[-self.length - 1:-1], dtype=float)

    @staticmethod
    def _line_price(line, x_value):
        return line[1] + x_value * line[0]

    def _count_touches(self, window, line, atr):
        """Считает количество точек окна, которые касаются линии (в пределах tolerance)."""
        tolerance = 0.1 * atr
        touches = 0
        for i, val in enumerate(window):
            line_val = self._line_price(line, i)
            if abs(val - line_val) <= tolerance:
                touches += 1
        return touches

    def _get_line(self, window, slope, value_index, is_resistance):
        slope_step = (window.max() - window.min()) / len(window)
        step = 1
        min_step = 0.0001
        best_slope = slope
        best_value = self._check_trend(window, slope, value_index, is_resistance)
        get_der = True
        der = None

        while step > min_step:
            if get_der:
                slope_change = best_slope + slope_step * min_step
                test_value = self._check_trend(window, slope_change, value_index, is_resistance)
                if test_value < 0:
                    slope_change = best_slope - slope_step * min_step
                    test_value = self._check_trend(window, slope_change, value_index, is_resistance)
                der = test_value - best_value
                get_der = False

            if der > 0:
                test_slope = best_slope - slope_step * step
            else:
                test_slope = best_slope + slope_step * step

            test_value = self._check_trend(window, test_slope, value_index, is_resistance)
            if test_value < 0 or test_value >= best_value:
                step *= 0.5
            else:
                best_slope = test_slope
                best_value = test_value
                get_der = True

        return best_slope, -best_slope * value_index + window[value_index]

    def _check_trend(self, window, slope, value_index, is_resistance):
        intercept = -slope * value_index + window[value_index]
        line_vals = slope * np.arange(len(window)) + intercept
        diffs = line_vals - window
        if (not is_resistance and diffs.max() > 0) or (is_resistance and diffs.min() < 0):
            return -1
        return float((diffs ** 2).sum())

    def get_atr(self):
        high = np.array(self.high, dtype=float)
        low = np.array(self.low, dtype=float)
        close = np.array(self.close, dtype=float)
        return _calc_atr(high, low, close, self.atr_length)

    def get_atr_at(self, offset: int = 0):
        """ATR на N баров назад от текущего. offset=-5 = ATR 5 баров назад."""
        if offset >= 0:
            return self.get_atr()
        end = len(self.high) + offset
        if end < self.atr_length:
            return None
        high = np.array(list(self.high)[:end], dtype=float)
        low = np.array(list(self.low)[:end], dtype=float)
        close = np.array(list(self.close)[:end], dtype=float)
        return _calc_atr(high, low, close, self.atr_length)


def _calc_atr(high, low, close, period):
    """Wilder's ATR — полный аналог talib.ATR, без внешних зависимостей."""
    if period <= 0 or len(close) < period:
        return float("nan")

    prev_close = np.empty_like(close)
    prev_close[0] = close[0]
    prev_close[1:] = close[:-1]

    true_range = np.maximum(
        high - low,
        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
    )

    # Wilder's smoothing: SMA для первого значения, затем EMA
    atr = true_range[:period].mean()
    for i in range(period, len(true_range)):
        atr = (atr * (period - 1) + true_range[i]) / period

    return float(atr)
