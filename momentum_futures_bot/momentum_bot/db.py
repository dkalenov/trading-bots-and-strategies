"""Database models and CRUD for momentum trading bot.
PostgreSQL via asyncpg + SQLAlchemy 2.0 async.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Boolean, DateTime,
    select, update, func,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


# ============================================================
# ORM Models
# ============================================================
class Base(DeclarativeBase):
    pass


class Config(Base):
    """Key-value config stored in DB (api_key, api_secret, trade_mode, etc)."""
    __tablename__ = "config"
    key = Column(String, primary_key=True)
    value = Column(String)


class Trade(Base):
    """One trading position from entry to full close."""
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    position_open = Column(Boolean, default=True)
    interval = Column(String, default="4h")
    side = Column(Boolean)                 # True=LONG, False=SHORT
    status = Column(String, default="NEW") # NEW, CLOSED_STOP, CLOSED_TAKE, CLOSED_BREAKEVEN, CLOSED_MARKET
    leverage = Column(Integer, default=20)
    order_size = Column(Float)             # notional in USDT

    # Timing
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)

    # Prices
    entry_price = Column(Float)
    quantity = Column(Float)
    take1_price = Column(Float)
    take2_price = Column(Float)
    stop_price = Column(Float)
    breakeven_stop_price = Column(Float)

    # Partial close
    take1_triggered = Column(Boolean, default=False)
    partial_exit_done = Column(Boolean, default=False)

    # ATR
    atr = Column(Float)
    atr_length = Column(Integer, default=14)

    # Result
    result = Column(Float, default=0)      # total realized PnL in USDT

    # V4.4 additions
    signal_score = Column(Float)           # composite score from structured engine
    risk_multiplier = Column(Float)        # adaptive sizing multiplier (0.5-1.5)
    exit_reason = Column(String)           # STOP, TAKE2, BREAKEVEN, MARKET
    initial_risk = Column(Float)           # $ risk at entry
    r_multiple = Column(Float)             # realized PnL / initial_risk
    signal_components = Column(String)     # JSON: {trend, momentum, timing, ...}

    # Telegram
    msg_id = Column(Integer)


class Order(Base):
    """Individual Binance order linked to a trade."""
    __tablename__ = "orders"
    order_id = Column(BigInteger, primary_key=True)
    trade_id = Column(Integer)
    symbol = Column(String)
    time = Column(BigInteger)
    side = Column(Boolean)      # True=BUY, False=SELL
    type = Column(String)       # MARKET, LIMIT, STOP_MARKET
    status = Column(String)     # NEW, FILLED, CANCELED, EXPIRED
    reduce = Column(Boolean)
    price = Column(Float)
    quantity = Column(Float)
    realized_profit = Column(Float, default=0.0)


class Universe(Base):
    """Active trading symbols."""
    __tablename__ = "universe"
    symbol = Column(String, primary_key=True)
    active = Column(Boolean, default=True)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))


class EquityLog(Base):
    """Periodic equity snapshots."""
    __tablename__ = "equity_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    equity = Column(Float)
    open_positions = Column(Integer)
    unrealized_pnl = Column(Float, default=0.0)


# BotState table removed — close_request signals now stored in Config table (same key-value schema)
# Old BotState rows are harmless; new DB installs won't have the table.


# ============================================================
# Connection
# ============================================================
Session: async_sessionmaker[AsyncSession] | None = None
_engine = None


async def connect(host: str, port: int, user: str, password: str, dbname: str):
    """Connect to PostgreSQL and return session factory."""
    global Session, _engine
    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
    _engine = create_async_engine(
        url, echo=False,
        pool_size=3,       # bot is single-process; 3 concurrent sessions is plenty
        max_overflow=7,    # allow burst up to 10 total, no more
        pool_timeout=10,
        pool_recycle=3600, # recycle idle connections every hour (prevents stale conns on VPS)
    )
    Session = async_sessionmaker(bind=_engine, expire_on_commit=False, class_=AsyncSession)
    return Session


async def create_tables():
    """Create all tables if they don't exist."""
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB tables created/verified.")


async def close():
    """Close engine."""
    if _engine:
        await _engine.dispose()


# ============================================================
# Config CRUD
# ============================================================
class ConfigInfo:
    """All bot config loaded from DB. Single source of truth — no config.json."""
    # API / connection
    api_key: str = None
    api_secret: str = None
    trade_mode: int = 0
    testnet: int = 1

    # Runtime params (changeable via TG, apply on next candle)
    leverage: int = 20
    risk_per_trade: float = 0.01
    max_positions: int = 3       # D3: was 5 (OOS PF 1.206 vs 1.044)
    max_entries_per_bar: int = 1  # D3: was 2
    symbol_cooldown: int = 3

    # Timeframe / signal mode
    interval: str = "4h"
    signal_mode: str = "structured"

    # Exit / ATR params
    stop_atr: float = 1.5
    take1_atr: float = 2.0
    take2_atr: float = 4.0
    partial_exit_pct: float = 0.10

    # Adaptive sizing
    use_adaptive_sizing: int = 1   # 1=on, 0=off
    adaptive_min_mult: float = 0.5
    adaptive_max_mult: float = 1.5

    # Structured signal thresholds
    struct_trend_min: float = 0.50
    struct_momentum_min: float = 0.40
    struct_timing_min: float = 0.25
    struct_composite_min: float = 0.25
    struct_weight_profile: str = "v41"

    # Hard cap on position notional (0 = disabled)
    manual_equity: float = 0.0

    def __init__(self, data: dict):
        for key, value in data.items():
            if not hasattr(self, key):
                # skip unknown keys (close_request_*, future keys, etc.)
                continue
            if value is None:
                setattr(self, key, None)
                continue
            # Preserve str fields
            if key in ("api_key", "api_secret", "interval", "signal_mode", "struct_weight_profile"):
                setattr(self, key, str(value))
                continue
            try:
                value = int(value)
            except (ValueError, TypeError):
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    setattr(self, key, value)
                    continue
            setattr(self, key, value)

    @property
    def has_keys(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def to_strategy_config(self):
        """Build a StrategyConfig from DB values for signal engine and risk manager."""
        from momentum_core.strategy import StrategyConfig
        return StrategyConfig(
            timeframe=str(self.interval or "4h"),
            signal_mode=str(self.signal_mode or "structured"),
            stop_atr=float(self.stop_atr or 1.5),
            take1_atr=float(self.take1_atr or 2.0),
            take2_atr=float(self.take2_atr or 4.0),
            partial_exit_pct=float(self.partial_exit_pct or 0.10),
            risk_per_trade=float(self.risk_per_trade or 0.01),
            max_positions=int(self.max_positions or 5),
            max_new_entries_per_bar=int(self.max_entries_per_bar or 2),
            symbol_cooldown_bars=int(self.symbol_cooldown or 3),
            use_adaptive_sizing=bool(self.use_adaptive_sizing),
            adaptive_min_mult=float(self.adaptive_min_mult or 0.5),
            adaptive_max_mult=float(self.adaptive_max_mult or 1.5),
            struct_trend_min=float(self.struct_trend_min or 0.50),
            struct_momentum_min=float(self.struct_momentum_min or 0.40),
            struct_timing_min=float(self.struct_timing_min or 0.25),
            struct_composite_min=float(self.struct_composite_min or 0.25),
            struct_weight_profile=str(self.struct_weight_profile or "v41"),
        )

    def to_dict(self) -> dict:
        """Backward-compat dict for RiskManager / PositionManager constructors."""
        return {
            "interval":             str(self.interval or "4h"),
            "signal_mode":          str(self.signal_mode or "structured"),
            "stop_atr":             float(self.stop_atr or 1.5),
            "take1_atr":            float(self.take1_atr or 2.0),
            "take2_atr":            float(self.take2_atr or 4.0),
            "partial_exit_pct":     float(self.partial_exit_pct or 0.10),
            "risk_per_trade":       float(self.risk_per_trade or 0.01),
            "max_positions":        int(self.max_positions or 5),
            "max_entries_per_bar":  int(self.max_entries_per_bar or 2),
            "symbol_cooldown":      int(self.symbol_cooldown or 3),
            "use_adaptive_sizing":  bool(self.use_adaptive_sizing),
            "adaptive_min_mult":    float(self.adaptive_min_mult or 0.5),
            "adaptive_max_mult":    float(self.adaptive_max_mult or 1.5),
            "struct_trend_min":     float(self.struct_trend_min or 0.50),
            "struct_momentum_min":  float(self.struct_momentum_min or 0.40),
            "struct_timing_min":    float(self.struct_timing_min or 0.25),
            "struct_composite_min": float(self.struct_composite_min or 0.25),
        }


async def ensure_defaults():
    """Seed default config keys and universe on first run. Safe to call every startup."""
    defaults = {
        # API / connection
        "api_key":            None,
        "api_secret":         None,
        "trade_mode":         "0",
        "testnet":            "1",
        # Runtime params (changeable via TG)
        "leverage":           "20",
        "risk_per_trade":     "0.01",
        "max_positions":      "3",   # D3: was 5
        "max_entries_per_bar": "1",  # D3: was 2
        "symbol_cooldown":    "3",
        # Timeframe / signal
        "interval":           "4h",
        "signal_mode":        "structured",
        # Exit / ATR
        "stop_atr":           "1.5",
        "take1_atr":          "2.0",
        "take2_atr":          "4.0",
        "partial_exit_pct":   "0.10",
        # Adaptive sizing
        "use_adaptive_sizing": "1",
        "adaptive_min_mult":  "0.5",
        "adaptive_max_mult":  "1.5",
        # Structured signal thresholds
        "struct_trend_min":    "0.50",
        "struct_momentum_min": "0.40",
        "struct_timing_min":   "0.25",
        "struct_composite_min": "0.25",
        "struct_weight_profile": "v41",
        # Manual equity override: if set, bot uses this instead of Binance balance.
        # Useful for testnet (gives $10k+) or small-deposit simulation.
        # 0 = use real Binance totalWalletBalance.
        "manual_equity": "0",
    }
    async with Session() as s:
        for key, default in defaults.items():
            stmt = insert(Config).values(key=key, value=default).on_conflict_do_nothing()
            await s.execute(stmt)
        await s.commit()
    logger.info("DB defaults seeded.")

    # Seed universe only if completely empty (don't override user's custom list)
    async with Session() as s:
        result = await s.execute(select(func.count()).select_from(Universe).where(Universe.active.is_(True)))
        active_count = result.scalar()
    if active_count == 0:
        logger.info(f"Universe empty — seeding {len(DEFAULT_SYMBOLS)} default symbols.")
        await reset_symbols()
    else:
        logger.info(f"Universe already has {active_count} active symbols — keeping existing.")



async def load_config() -> ConfigInfo:
    """Load all config keys from DB (pure SELECT, no writes)."""
    async with Session() as s:
        result = await s.execute(select(Config))
        data = {row.key: row.value for row in result.scalars()}
    return ConfigInfo(data)


async def config_update(**kwargs):
    """Update config keys in DB."""
    async with Session() as s:
        for key, value in kwargs.items():
            await s.execute(
                insert(Config).values(key=key, value=str(value))
                .on_conflict_do_update(index_elements=["key"], set_={"value": str(value)})
            )
        await s.commit()


# ============================================================
# Universe CRUD
# ============================================================
# Top-20 symbols by PnL from per_symbol_wide_4h_20240101_20260418 backtest.
# These were selected by per-symbol optimization WITHOUT slot competition.
# DO NOT replace with structured-scan universe — it overfits (see skill rules).
DEFAULT_SYMBOLS = [
    "BRUSDT",       # PnL=12860  PF=10.6
    "HBARUSDT",     # PnL=11702  PF=1.70
    "DOGEUSDT",     # PnL=11606  PF=1.52
    "XVGUSDT",      # PnL=10387  PF=2.41
    "1000PEPEUSDT", # PnL=9094   PF=1.62
    "RSRUSDT",      # PnL=9005   PF=1.48
    "WIFUSDT",      # PnL=8871   PF=1.71
    "ARUSDT",       # PnL=8729   PF=1.60
    "STGUSDT",      # PnL=8711   PF=1.74
    "DYMUSDT",      # PnL=8640   PF=1.92
    "1000FLOKIUSDT",# PnL=8622   PF=2.09
    "ONEUSDT",      # PnL=8534   PF=1.91
    "JASMYUSDT",    # PnL=8042   PF=1.60
    "RONINUSDT",    # PnL=7884   PF=1.67
    "LQTYUSDT",     # PnL=7872   PF=1.78
    "WLDUSDT",      # PnL=7844   PF=1.63
    "METISUSDT",    # PnL=7520   PF=1.64
    "PORTALUSDT",   # PnL=7401   PF=2.07
    "AVAXUSDT",     # PnL=7002   PF=1.62
    "ATAUSDT",      # PnL=6864   PF=1.59
]


async def get_active_symbols() -> list[str]:
    """Get list of active trading symbols."""
    async with Session() as s:
        result = await s.execute(
            select(Universe.symbol).where(Universe.active.is_(True)).order_by(Universe.symbol)
        )
        return [row[0] for row in result.all()]


async def add_symbol(symbol: str) -> bool:
    """Add symbol to universe. Returns True if new."""
    async with Session() as s:
        stmt = insert(Universe).values(
            symbol=symbol.upper(), active=True,
            added_at=datetime.now(timezone.utc).replace(tzinfo=None),
        ).on_conflict_do_update(index_elements=["symbol"], set_={"active": True})
        await s.execute(stmt)
        await s.commit()
    return True


async def remove_symbol(symbol: str) -> bool:
    """Deactivate symbol from universe."""
    async with Session() as s:
        result = await s.execute(
            update(Universe).where(Universe.symbol == symbol.upper()).values(active=False)
        )
        await s.commit()
        return result.rowcount > 0


async def reset_symbols():
    """Reset universe to default 20 symbols."""
    async with Session() as s:
        await s.execute(update(Universe).values(active=False))
        for sym in DEFAULT_SYMBOLS:
            stmt = insert(Universe).values(
                symbol=sym, active=True,
                added_at=datetime.now(timezone.utc).replace(tzinfo=None),
            ).on_conflict_do_update(index_elements=["symbol"], set_={"active": True})
            await s.execute(stmt)
        await s.commit()


# ============================================================
# Trade CRUD
# ============================================================
async def get_open_trade(symbol: str) -> Trade | None:
    """Get open trade for symbol."""
    async with Session() as s:
        result = await s.execute(
            select(Trade).where(
                Trade.symbol == symbol, Trade.position_open.is_(True)
            ).order_by(Trade.open_time.desc()).limit(1)
        )
        return result.scalar_one_or_none()


async def get_all_open_trades() -> list[Trade]:
    """Get all open trades."""
    async with Session() as s:
        result = await s.execute(
            select(Trade).where(Trade.position_open.is_(True))
        )
        return result.scalars().all()


async def save_trade(trade: Trade) -> Trade:
    """Insert new trade and return with ID."""
    async with Session() as s:
        s.add(trade)
        await s.commit()
        await s.refresh(trade)
    return trade


async def update_trade(trade_id: int, **kwargs):
    """Update trade fields."""
    async with Session() as s:
        await s.execute(update(Trade).where(Trade.id == trade_id).values(**kwargs))
        await s.commit()


async def close_trade(trade_id: int, status: str, exit_reason: str):
    """Mark trade as closed."""
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    async with Session() as s:
        await s.execute(
            update(Trade).where(Trade.id == trade_id).values(
                position_open=False, status=status,
                close_time=now, exit_reason=exit_reason,
            )
        )
        await s.commit()


async def get_last_trade_close_time(symbol: str) -> int | None:
    """Get close_time of last closed trade for cooldown check."""
    async with Session() as s:
        result = await s.execute(
            select(Trade.close_time).where(
                Trade.symbol == symbol, Trade.position_open.is_(False)
            ).order_by(Trade.close_time.desc()).limit(1)
        )
        row = result.scalar_one_or_none()
        return row


async def get_recent_trades(limit: int = 20) -> list[Trade]:
    """Get last N closed trades."""
    async with Session() as s:
        result = await s.execute(
            select(Trade).where(Trade.position_open.is_(False))
            .order_by(Trade.close_time.desc()).limit(limit)
        )
        return result.scalars().all()


async def get_trade_stats() -> dict:
    """Get aggregate stats from all closed trades."""
    async with Session() as s:
        result = await s.execute(
            select(
                func.count(Trade.id).label("total"),
                func.sum(Trade.result).label("total_pnl"),
                func.avg(Trade.result).label("avg_pnl"),
                func.max(Trade.result).label("best"),
                func.min(Trade.result).label("worst"),
            ).where(Trade.position_open.is_(False))
        )
        row = result.one()
        total = row.total or 0
        if total == 0:
            return {"total": 0, "pnl": 0, "wins": 0, "losses": 0, "wr": 0, "pf": 0, "avg_pnl": 0}

        wins_result = await s.execute(
            select(func.count(Trade.id), func.sum(Trade.result))
            .where(Trade.position_open.is_(False), Trade.result > 0)
        )
        w_row = wins_result.one()
        losses_result = await s.execute(
            select(func.count(Trade.id), func.sum(func.abs(Trade.result)))
            .where(Trade.position_open.is_(False), Trade.result <= 0)
        )
        l_row = losses_result.one()

        gross_profit = w_row[1] or 0
        gross_loss = l_row[1] or 0
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        return {
            "total": total, "pnl": round(row.total_pnl or 0, 2),
            "wins": w_row[0] or 0, "losses": l_row[0] or 0,
            "wr": round((w_row[0] or 0) / total * 100, 1),
            "pf": round(pf, 3),
            "avg_pnl": round(row.avg_pnl or 0, 4),
            "best": round(row.best or 0, 2), "worst": round(row.worst or 0, 2),
        }


# ============================================================
# Order CRUD
# ============================================================
async def save_order(order: Order):
    """Insert or update order."""
    async with Session() as s:
        stmt = insert(Order).values(
            order_id=order.order_id, trade_id=order.trade_id,
            symbol=order.symbol, time=order.time,
            side=order.side, type=order.type, status=order.status,
            reduce=order.reduce, price=order.price,
            quantity=order.quantity, realized_profit=order.realized_profit,
        ).on_conflict_do_update(
            index_elements=["order_id"],
            set_={"status": order.status, "price": order.price,
                   "quantity": order.quantity, "realized_profit": order.realized_profit,
                   "time": order.time},
        )
        await s.execute(stmt)
        await s.commit()


async def get_order_trade(order_id: int) -> tuple[Order | None, Trade | None]:
    """Get order and associated trade."""
    async with Session() as s:
        order = (await s.execute(
            select(Order).where(Order.order_id == order_id)
        )).scalar_one_or_none()
        trade = None
        if order:
            trade = (await s.execute(
                select(Trade).where(Trade.id == order.trade_id)
            )).scalar_one_or_none()
        return order, trade


async def get_last_active_stop_order(trade_id: int) -> Order | None:
    """Get last active stop order for trade."""
    async with Session() as s:
        result = await s.execute(
            select(Order).where(
                Order.trade_id == trade_id,
                Order.type == "STOP_MARKET",
                Order.reduce.is_(True),
                Order.status == "NEW",
            ).order_by(Order.time.desc())
        )
        return result.scalars().first()  # .first() safe even if multiple rows exist


async def get_active_reduce_orders(trade_id: int) -> list[Order]:
    """Get active reduce-only orders for a trade."""
    async with Session() as s:
        result = await s.execute(
            select(Order).where(
                Order.trade_id == trade_id,
                Order.reduce.is_(True),
                Order.status.in_(("NEW", "PARTIALLY_FILLED")),
            ).order_by(Order.time.desc())
        )
        return result.scalars().all()


async def update_trade_result(trade_id: int):
    """Recalculate trade result from orders."""
    async with Session() as s:
        result = await s.execute(
            select(func.sum(Order.realized_profit)).where(Order.trade_id == trade_id)
        )
        total = result.scalar() or 0.0
        await s.execute(
            update(Trade).where(Trade.id == trade_id).values(result=total)
        )
        await s.commit()


async def get_total_realized_pnl() -> float:
    """Get sum of realized PnL from all closed trades (SQL-level, BUG-10)."""
    async with Session() as s:
        result = await s.execute(
            select(func.coalesce(func.sum(Trade.result), 0.0))
            .where(Trade.position_open.is_(False))
        )
        return float(result.scalar())


# ============================================================
# Equity Log
# ============================================================
async def log_equity(equity: float, open_positions: int, unrealized_pnl: float = 0.0):
    """Save equity snapshot."""
    async with Session() as s:
        s.add(EquityLog(
            equity=equity, open_positions=open_positions,
            unrealized_pnl=unrealized_pnl,
        ))
        await s.commit()


async def get_equity_logs(limit: int = 24) -> list[EquityLog]:
    """Get last N equity snapshots for display."""
    async with Session() as s:
        result = await s.execute(
            select(EquityLog).order_by(EquityLog.timestamp.desc()).limit(limit)
        )
        return list(reversed(result.scalars().all()))


# ============================================================
# Bot State — stored in Config table (same key-value schema, no separate table needed)
# ============================================================
async def set_state(key: str, value: str):
    """Set a transient state value (e.g. close_request_SYMBOL). Uses Config table."""
    await config_update(**{key: value})


async def get_state(key: str) -> str | None:
    """Get a transient state value from Config table."""
    async with Session() as s:
        result = await s.execute(select(Config.value).where(Config.key == key))
        return result.scalar_one_or_none()
