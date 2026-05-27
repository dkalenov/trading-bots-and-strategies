"""Live signal engine wrapper around momentum_core strategies.

Production must reuse the same signal logic as research/backtests; otherwise
the bot can trade a different model than the one that was validated.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from momentum_core.strategy import StrategyConfig, create_strategy

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """Normalized live signal payload for the execution layer."""

    symbol: str
    direction: int          # 1=LONG, -1=SHORT
    score: float
    components: dict
    atr: float
    entry_price: float


class LiveSignalEngine:
    """Compute live signals using the same strategy classes as backtests."""

    def __init__(self, config: "dict | StrategyConfig | None" = None):
        if isinstance(config, StrategyConfig):
            # Already a typed config — use directly (preferred path from DB)
            self.config = config
        else:
            # Legacy dict path: filter to valid StrategyConfig keys only
            raw = dict(config or {})
            valid_keys = set(StrategyConfig.__dataclass_fields__.keys())
            filtered = {k: v for k, v in raw.items() if k in valid_keys}
            self.config = StrategyConfig(**filtered)
        self.strategy = create_strategy(self.config)

    def compute_signals(
        self,
        symbol_frames: dict[str, pd.DataFrame],
        btc_frame: pd.DataFrame,
    ) -> list[Signal]:
        """Compute live entry candidates across all symbols."""
        if not symbol_frames or btc_frame is None or btc_frame.empty:
            return []

        try:
            btc_regime = self.strategy.build_btc_regime(btc_frame)
        except Exception as exc:
            logger.error("BTC regime build failed: %s", exc)
            return []

        signals: list[Signal] = []
        for symbol, frame in symbol_frames.items():
            try:
                signal = self._compute_one(symbol, frame, btc_regime)
                if signal is not None:
                    signals.append(signal)
            except Exception as exc:
                logger.error("Signal error %s: %s", symbol, exc)

        signals.sort(key=lambda s: s.score, reverse=True)
        return signals

    def _compute_one(
        self,
        symbol: str,
        frame: pd.DataFrame,
        btc_regime: pd.DataFrame,
    ) -> Signal | None:
        if frame is None or frame.empty or len(frame) < 100:
            return None

        prepared = self.strategy.prepare_symbol(symbol, frame, btc_regime)
        if prepared is None or prepared.empty:
            return None

        last = prepared.iloc[-1]
        direction = int(last.get("signal_direction", 0) or 0)
        if direction == 0:
            return None

        atr = float(last.get("atr14", 0.0) or 0.0)
        if atr <= 0:
            return None

        entry_price = float(last.get("close", frame["close"].iloc[-1]))
        score = float(last.get("signal_score", 0.0) or 0.0)

        return Signal(
            symbol=symbol,
            direction=direction,
            score=score,
            components=self._extract_components(last, direction),
            atr=atr,
            entry_price=entry_price,
        )

    def _extract_components(self, row: pd.Series, direction: int) -> dict:
        """Expose the most useful model diagnostics for logs/DB."""
        prefix = "long" if direction == 1 else "short"

        components = {
            "trend": float(row.get(f"{prefix}_trend", 0.0) or 0.0),
            "momentum": float(row.get(f"{prefix}_momentum", 0.0) or 0.0),
            "timing": float(row.get(f"{prefix}_timing", 0.0) or 0.0),
            "volume": float(row.get("volume_score", 0.0) or 0.0),
            "relative": float(row.get(f"{prefix}_rs", 0.0) or 0.0),
            "setup": float(row.get("setup_quality", 0.0) or 0.0),
            "composite": float(row.get(f"{prefix}_composite", 0.0) or 0.0),
            "obv": float(row.get(f"{prefix}_obv", 0.0) or 0.0),
            "donchian": float(row.get(f"{prefix}_donchian", 0.0) or 0.0),
            "freshness": float(row.get(f"{prefix}_freshness", 0.0) or 0.0),
            "reason": str(row.get("signal_reason", "")),
        }
        return components
