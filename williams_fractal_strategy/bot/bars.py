"""Rolling window of CLOSED bars per symbol.

Only closed candles ever go in here — a partially-formed candle is not
a completed bar and must never be fed to the fractal signal logic
(that would be a live look-ahead/repaint bug: the signal could flip as
the candle continues forming). The websocket handler is the gatekeeper
for this — it only calls add_closed_bar() when Binance's kline event
carries "x": true.
"""
from __future__ import annotations

from collections import deque


class BarWindow:
    def __init__(self, maxlen: int = 500):
        self._bars: deque[dict] = deque(maxlen=maxlen)

    def seed_from_rest_klines(self, klines: list[list]) -> None:
        """klines: raw REST /fapi/v1/klines response. The LAST entry from
        this endpoint is usually the still-forming candle — drop it."""
        self._bars.clear()
        rows = klines[:-1] if klines else klines
        for k in rows:
            self._bars.append({
                "open_time": k[0], "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
            })

    def add_closed_bar(self, open_time: int, o: float, h: float, l: float, c: float, v: float) -> None:
        if self._bars and self._bars[-1]["open_time"] == open_time:
            self._bars[-1] = {"open_time": open_time, "open": o, "high": h, "low": l, "close": c, "volume": v}
        else:
            self._bars.append({"open_time": open_time, "open": o, "high": h, "low": l, "close": c, "volume": v})

    def bars(self) -> list[dict]:
        return list(self._bars)

    def __len__(self) -> int:
        return len(self._bars)
