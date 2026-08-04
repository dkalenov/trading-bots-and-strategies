"""
Strategy layer: turns a rolling window of closed bars into a
directional signal. Decoupled from execution (main.py) the same way
algofactory_bot separates strategies/ from execution/ — this module
only ever answers "is there a signal", never places an order or knows
about the exchange.

The actual signal logic is imported unchanged from ../src/fractals.py
— this is not a reimplementation, it's the exact function
run_backtest.py uses, so a live run and a backtest agree by
construction rather than by two implementations happening to match.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pandas as pd  # noqa: E402
from fractals import generate_signals  # noqa: E402
from indicators import compute_atr  # noqa: E402


@dataclass
class SignalResult:
    ready: bool                          # enough bars to evaluate at all
    direction: int = 0                    # 1 long, -1 short, 0 none
    structure_stop: Decimal | None = None  # the swing point the signal formed against
    atr_value: Decimal | None = None
    forced: bool = False                  # True if this came from debug-mode forcing


def check_signal(
    bars: list[dict],
    *,
    fractal_n: int,
    warmup_bars: int,
    atr_period: int,
    force_debug_signal: bool = False,
) -> SignalResult:
    """
    bars: closed candles only, oldest first, each a dict with at least
    open/high/low/close (see bars.BarWindow).

    force_debug_signal: if True and no real signal is found, force a
    LONG signal — this is DEBUG_MODE's job in main.py, this function
    just does what it's told; main.py decides whether/when to force
    and never forces more than once per run.
    """
    if len(bars) < warmup_bars:
        return SignalResult(ready=False)

    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]

    df = pd.DataFrame({"high": highs, "low": lows})
    result = generate_signals(df, n=fractal_n)
    last = result.iloc[-1]
    direction = int(last["signal"])
    structure_stop = None
    if direction != 0 and not pd.isna(last["stop_level"]):
        structure_stop = Decimal(str(last["stop_level"]))

    forced = False
    if direction == 0 and force_debug_signal:
        direction = 1
        forced = True

    if direction == 0:
        return SignalResult(ready=True, direction=0)

    atr_series = compute_atr(pd.DataFrame({"high": highs, "low": lows, "close": closes}), period=atr_period)
    atr_raw = atr_series.iloc[-1]
    atr_value = Decimal(str(atr_raw)) if atr_raw == atr_raw else None  # NaN check

    return SignalResult(
        ready=True, direction=direction, structure_stop=structure_stop,
        atr_value=atr_value, forced=forced,
    )
