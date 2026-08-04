"""
Bot configuration. Loaded from environment variables (and a .env file
if present) so no secrets ever live in a committed file.

Deliberately a single flat EXECUTION_MODE, unlike algofactory_bot's
EXECUTION_MODE + EXCHANGE_ENV pair (see that project's
VERIFICATION_NOTES.md 3b — that split caused a real, confirmed bug
where testnet-only intent silently fell through to production
endpoints). Here, one setting fully determines the base URL:

    dry_run  -> no network calls at all, in-memory simulated exchange
    testnet  -> real Binance Futures TESTNET endpoints, fake funds
    live     -> real Binance Futures production endpoints, REAL MONEY
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from utils import load_dotenv

VALID_EXECUTION_MODES = ("dry_run", "testnet", "live")

REST_BASE_URLS = {
    "testnet": "https://testnet.binancefuture.com",
    "live": "https://fapi.binance.com",
}
WS_BASE_URLS = {
    "testnet": "wss://stream.binancefuture.com",
    "live": "wss://fstream.binance.com",
}


def _env_list(name: str, default: str) -> list[str]:
    raw = os.environ.get(name, default)
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class BotConfig:
    execution_mode: str
    symbols: list[str]
    interval: str

    # signal generation — same knobs as run_backtest.py, so a live run
    # and a backtest run of the same parameters do the same thing
    fractal_n: int
    warmup_bars: int

    # risk / stop / take — same names and meaning as backtest.run_backtest()
    risk_per_trade: float
    stop_mode: str            # "structure" | "atr" | "percent"
    atr_period: int
    atr_multiplier: float
    stop_pct: float
    reward_risk_ratio: float
    max_leverage: float
    exchange_leverage: int    # leverage actually SET on the exchange for the symbol
    min_stop_pct: float
    max_stop_pct: float

    max_positions: int
    poll_reconcile_seconds: int
    debug_mode: bool

    api_key: str = field(repr=False, default="")
    api_secret: str = field(repr=False, default="")

    @property
    def rest_base_url(self) -> str:
        return REST_BASE_URLS.get(self.execution_mode, REST_BASE_URLS["testnet"])

    @property
    def ws_base_url(self) -> str:
        return WS_BASE_URLS.get(self.execution_mode, WS_BASE_URLS["testnet"])

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> "BotConfig":
        load_dotenv(Path(env_file) if env_file else Path(__file__).resolve().parent / ".env")

        execution_mode = os.environ.get("EXECUTION_MODE", "dry_run").strip().lower()
        if execution_mode not in VALID_EXECUTION_MODES:
            raise ValueError(
                f"EXECUTION_MODE must be one of {VALID_EXECUTION_MODES}, got {execution_mode!r}"
            )

        stop_mode = os.environ.get("STOP_MODE", "structure").strip().lower()
        if stop_mode not in ("structure", "atr", "percent"):
            raise ValueError("STOP_MODE must be 'structure', 'atr' or 'percent'")

        cfg = cls(
            execution_mode=execution_mode,
            symbols=_env_list("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"),
            interval=os.environ.get("INTERVAL", "4h"),
            fractal_n=_env_int("FRACTAL_N", 2),
            warmup_bars=_env_int("WARMUP_BARS", 60),
            risk_per_trade=_env_float("RISK_PER_TRADE", 0.01),
            stop_mode=stop_mode,
            atr_period=_env_int("ATR_PERIOD", 14),
            atr_multiplier=_env_float("ATR_MULTIPLIER", 1.5),
            stop_pct=_env_float("STOP_PCT", 0.02),
            reward_risk_ratio=_env_float("REWARD_RISK_RATIO", 2.0),
            max_leverage=_env_float("MAX_LEVERAGE", 1.0),
            exchange_leverage=_env_int("EXCHANGE_LEVERAGE", 5),
            min_stop_pct=_env_float("MIN_STOP_PCT", 0.001),
            max_stop_pct=_env_float("MAX_STOP_PCT", 0.15),
            max_positions=_env_int("MAX_POSITIONS", 5),
            poll_reconcile_seconds=_env_int("POLL_RECONCILE_SECONDS", 120),
            debug_mode=_env_bool("DEBUG_MODE", False),
            api_key=os.environ.get("BINANCE_API_KEY", ""),
            api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        )

        if cfg.execution_mode != "dry_run" and not (cfg.api_key and cfg.api_secret):
            raise ValueError(
                f"EXECUTION_MODE={cfg.execution_mode} requires BINANCE_API_KEY and "
                f"BINANCE_API_SECRET to be set (dry_run works without them)"
            )
        return cfg
