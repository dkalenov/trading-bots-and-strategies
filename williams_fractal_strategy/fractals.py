"""
Williams Fractal detection and swing-structure breakout signal generation.

A Williams Fractal is a 5-bar pattern (with default n=2):
  - fractal HIGH at bar i: high[i] is strictly greater than the high of
    the n bars before AND the n bars after it.
  - fractal LOW at bar i: low[i] is strictly lower than the low of the
    n bars before AND the n bars after it.

A fractal needs `n` bars *after* the pivot to be confirmed, so a fractal
at bar i is only actually knowable in real time at bar i + n. This
module tracks that confirmation delay everywhere so signals never use
information from the future (no look-ahead bias).

Signal logic (market-structure breakout):
  Bullish structure = a swing LOW followed by a swing HIGH followed by a
  swing LOW that is HIGHER than the first one ("higher low"). While that
  structure stands, a break above the swing HIGH in between is a
  LONG signal (bullish break of structure).

  Bearish structure = a swing HIGH followed by a swing LOW followed by a
  swing HIGH that is LOWER than the first one ("lower high"). While that
  structure stands, a break below the swing LOW in between is a
  SHORT signal (bearish break of structure).

  signal ==  1 -> LONG   (breakout above resistance, higher-low structure)
  signal == -1 -> SHORT  (breakdown below support, lower-high structure)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


def detect_fractals(df: pd.DataFrame, n: int = 2) -> pd.DataFrame:
    """Return a copy of df with boolean 'fractal_high' / 'fractal_low' columns."""
    df = df.copy()
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()
    length = len(df)

    fractal_high = np.zeros(length, dtype=bool)
    fractal_low = np.zeros(length, dtype=bool)

    for i in range(n, length - n):
        h_window = high[i - n : i + n + 1]
        if high[i] == h_window.max() and (h_window == high[i]).sum() == 1:
            fractal_high[i] = True

        l_window = low[i - n : i + n + 1]
        if low[i] == l_window.min() and (l_window == low[i]).sum() == 1:
            fractal_low[i] = True

    df["fractal_high"] = fractal_high
    df["fractal_low"] = fractal_low
    return df


@dataclass
class SwingPoint:
    kind: str  # "high" or "low"
    price: float
    pivot_idx: int
    confirm_idx: int  # bar index at which this pivot becomes knowable (pivot_idx + n)


def _build_zigzag(df: pd.DataFrame, n: int) -> list[SwingPoint]:
    """
    Turn raw fractal pivots into a strictly alternating zig-zag of swing
    points. Consecutive same-type pivots are consolidated into the most
    extreme one (the true swing high/low) rather than just the first one
    found, so the zig-zag reflects real swing structure.
    """
    points: list[SwingPoint] = []
    for i in range(len(df)):
        if df["fractal_high"].iat[i]:
            points.append(SwingPoint("high", df["high"].iat[i], i, i + n))
        if df["fractal_low"].iat[i]:
            points.append(SwingPoint("low", df["low"].iat[i], i, i + n))
    points.sort(key=lambda p: (p.confirm_idx, p.pivot_idx))

    zigzag: list[SwingPoint] = []
    for p in points:
        if zigzag and zigzag[-1].kind == p.kind:
            more_extreme = (
                p.price > zigzag[-1].price if p.kind == "high" else p.price < zigzag[-1].price
            )
            if more_extreme:
                zigzag[-1] = p
            continue
        zigzag.append(p)
    return zigzag


def generate_signals(df: pd.DataFrame, n: int = 2) -> pd.DataFrame:
    """
    Detect Williams Fractals and generate breakout signals from the
    resulting swing structure. See module docstring for the rule.

    Returns a copy of df with added columns:
      fractal_high, fractal_low   - bool, raw pivot flags
      signal                      - int, 1 = LONG, -1 = SHORT, 0 = none
      signal_level                - float, the level that was broken
      setup_type                  - str, human-readable reason
    """
    df = detect_fractals(df, n=n)
    zigzag = _build_zigzag(df, n=n)

    length = len(df)
    signal = np.zeros(length, dtype=int)
    signal_level = np.full(length, np.nan)
    stop_level = np.full(length, np.nan)
    setup_type = np.array([""] * length, dtype=object)

    confirm_at: dict[int, list[SwingPoint]] = {}
    for p in zigzag:
        confirm_at.setdefault(p.confirm_idx, []).append(p)

    window: list[SwingPoint] = []  # most recent (up to 3) confirmed swing points
    active_direction: int | None = None
    active_level: float | None = None
    active_stop: float | None = None  # the swing point that invalidates the setup

    high = df["high"].to_numpy()
    low = df["low"].to_numpy()

    for i in range(length):
        # 1) reveal any swing points that become confirmed at this bar
        for p in confirm_at.get(i, []):
            window.append(p)
            if len(window) > 3:
                window.pop(0)

            if len(window) == 3:
                a, b, c = window
                if a.kind == c.kind == "low" and c.price > a.price:
                    # bullish: breakout above b (resistance), invalidated
                    # below c (the higher low that defined the setup)
                    active_direction, active_level, active_stop = 1, b.price, c.price
                elif a.kind == c.kind == "high" and c.price < a.price:
                    # bearish: breakdown below b (support), invalidated
                    # above c (the lower high that defined the setup)
                    active_direction, active_level, active_stop = -1, b.price, c.price
                else:
                    active_direction, active_level, active_stop = None, None, None

        # 2) check breakout using only the current bar (no look-ahead:
        #    active_level was confirmed at or before bar i)
        if active_direction == 1 and high[i] > active_level:
            signal[i] = 1
            signal_level[i] = active_level
            stop_level[i] = active_stop
            setup_type[i] = "higher_low_breakout"
            window, active_direction, active_level, active_stop = [], None, None, None
        elif active_direction == -1 and low[i] < active_level:
            signal[i] = -1
            signal_level[i] = active_level
            stop_level[i] = active_stop
            setup_type[i] = "lower_high_breakdown"
            window, active_direction, active_level, active_stop = [], None, None, None

    df["signal"] = signal
    df["signal_level"] = signal_level
    df["stop_level"] = stop_level  # structure-based invalidation point for this signal
    df["setup_type"] = setup_type
    return df
