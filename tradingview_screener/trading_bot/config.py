"""
Configuration for the strategy math and for a running bot instance.
Split the way the reference architecture splits config.py from
config_loader.py: this file defines what a valid config looks like and
validates it, config_loader.py is the only place that reads env vars or
CLI args and turns them into one of these.
"""
from __future__ import annotations

from dataclasses import dataclass, field


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class StrategyConfig:
    """The strategy's own parameters - ATR length, exit multipliers,
    per-trade sizing. Values default to the dominant real settings found
    in the original project's database snapshots (see docs/AUDIT.md,
    H2), not arbitrary numbers."""
    atr_length: int = 14
    stop_mult: float = 0.45
    take1_mult: float = 2.5
    take2_mult: float = 5.0
    take1_portion: float = 0.05
    order_size_usd: float = 10.0
    breakeven_buffer: float = 0.001

    def __post_init__(self):
        if self.atr_length < 2:
            raise ConfigError(f"atr_length must be >= 2, got {self.atr_length}")
        if self.stop_mult <= 0 or self.take1_mult <= 0 or self.take2_mult <= 0:
            raise ConfigError("stop_mult, take1_mult, take2_mult must all be positive")
        if not (0 < self.take1_portion < 1):
            raise ConfigError(f"take1_portion must be between 0 and 1, got {self.take1_portion}")
        if self.order_size_usd <= 0:
            raise ConfigError(f"order_size_usd must be positive, got {self.order_size_usd}")


@dataclass(frozen=True)
class BotConfig:
    """Everything a running bot instance needs beyond the strategy math:
    what to trade, how fast to check it, dry-run/testnet/mainnet, where
    state lives."""
    watchlist: tuple[str, ...]
    interval: str = "4h"
    leverage: int = 20
    poll_interval_sec: int = 60
    dry_run: bool = True
    mainnet: bool = False
    db_path: str = "bot_state.sqlite3"
    strategy: StrategyConfig = field(default_factory=StrategyConfig)

    def __post_init__(self):
        if not self.watchlist:
            raise ConfigError("watchlist must not be empty")
        if self.leverage < 1 or self.leverage > 125:
            raise ConfigError(f"leverage out of realistic range: {self.leverage}")
        if self.poll_interval_sec < 1:
            raise ConfigError(f"poll_interval_sec must be positive, got {self.poll_interval_sec}")
