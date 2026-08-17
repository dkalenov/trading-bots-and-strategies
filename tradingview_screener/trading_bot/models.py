"""
Domain enums and data types, shared by every other module. Nothing in
here talks to a network, a database, or does math beyond what a
dataclass needs - it's the vocabulary the rest of the project is written
in, kept in one place so `PositionState.OPEN` means the same thing in
db.py, execution/position_manager.py, and strategies/tradingview_screener.py.

Exchange-boundary values (quantity, price) use Decimal - see utils.py's
module docstring for why plain float rounding is what caused the -1111
precision bug documented in docs/AUDIT.md (T1). Derived indicator values
(ATR) stay float, same convention as the reference architecture.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


# === Enums ===================================================================

class ExecutionMode(StrEnum):
    DRY_RUN = "dry_run"
    TESTNET = "testnet"
    LIVE = "live"


class PositionState(StrEnum):
    OPEN = "open"                 # entry filled, take1 not yet hit
    PROTECTED = "protected"       # take1 hit, stop moved to breakeven
    CLOSED = "closed"


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


# === Domain dataclasses ======================================================

@dataclass(frozen=True)
class Rating:
    """TradingView's technical rating for one symbol at one point in time."""
    symbol: str
    recommendation: str   # STRONG_BUY / BUY / NEUTRAL / SELL / STRONG_SELL
    buy_count: int = 0
    sell_count: int = 0
    neutral_count: int = 0


@dataclass(frozen=True)
class ExitLevels:
    """Stop/take levels for one position, computed once at entry."""
    stop: Decimal
    take1: Decimal
    take2: Decimal


@dataclass(frozen=True)
class SymbolFilters:
    """The subset of Binance's exchangeInfo filters this project needs."""
    step_size: float
    tick_size: float
    min_notional: float


@dataclass(frozen=True)
class PositionSizing:
    """Output of risk.RiskManager.compute_position_size()."""
    quantity: Decimal
    entry_price: Decimal
    stop: Decimal
    take1: Decimal
    take2: Decimal
    atr: float


@dataclass
class TradeRecord:
    """One position's full lifecycle, as persisted by db.py.

    entry_order_id / take2_order_id hold regular Binance orderIds.
    stop_order_id holds an algoId - STOP_MARKET is a conditional order,
    placed and cancelled through the separate Algo Order API (see
    exchange/binance/futures.py: new_algo_stop_market_order), whose
    response and lookup key is `algoId`, not `orderId`.
    """
    symbol: str
    direction: str
    entry_time: str
    entry_price: float
    atr: float
    stop: float
    take1: float
    take2: float
    qty_full: float
    qty_remaining: float
    take1_done: bool = False
    breakeven_stop: float | None = None
    status: str = PositionState.OPEN.value
    exit_reason: str | None = None
    pnl_usd: float | None = None
    closed_at: str | None = None
    entry_order_id: str | None = None
    stop_order_id: str | None = None
    take2_order_id: str | None = None
    id: int | None = None
