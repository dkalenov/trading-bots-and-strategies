"""
Database layer for trade state. Mirrors the reference architecture's
db.py shape - one declarative ORM model, module-level CRUD functions,
connect()/create_tables()/close() lifecycle - with two deliberate
differences, both explained below rather than silently done:

1. SQLite, not PostgreSQL. The reference project runs multiple bot
   instances against one shared operational database with run-identity
   bookkeeping (run_id, execution_mode, is_simulated, config_hash) - a
   real requirement for that scale. This project is one person running
   one bot instance; a Postgres server is a dependency with no payoff
   at that scale, and was itself one of the reasons the original
   tradingview_screener project couldn't be run by "anyone" (see
   docs/AUDIT.md). SQLAlchemy's ORM layer and query API are unchanged -
   only the engine URL differs (sqlite:/// vs postgresql+asyncpg://).

2. Synchronous, not async. The reference project is asyncio end to end.
   This bot's execution loop (execution/position_manager.py) is a plain
   polling loop, a deliberate simplification documented in that module -
   making just the database layer async while everything around it is
   sync would add asyncio's sharp edges (event loop management, session
   lifecycle across sync callers) for no actual concurrency benefit here.
"""
from __future__ import annotations

import sys
import os
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

sys.path.insert(0, os.path.dirname(__file__))
from models import TradeRecord


class Base(DeclarativeBase):
    pass


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False, index=True)
    direction = Column(String(8), nullable=False)
    entry_time = Column(String(40), nullable=False)
    entry_price = Column(Float, nullable=False)
    atr = Column(Float, nullable=False)
    stop = Column(Float, nullable=False)
    take1 = Column(Float, nullable=False)
    take2 = Column(Float, nullable=False)
    qty_full = Column(Float, nullable=False)
    qty_remaining = Column(Float, nullable=False)
    take1_done = Column(Boolean, nullable=False, default=False)
    breakeven_stop = Column(Float, nullable=True)
    status = Column(String(16), nullable=False, default="open", index=True)
    exit_reason = Column(String(24), nullable=True)
    pnl_usd = Column(Float, nullable=True)
    closed_at = Column(String(40), nullable=True)
    entry_order_id = Column(String(32), nullable=True)
    stop_order_id = Column(String(32), nullable=True)   # holds an algoId, see models.py
    take2_order_id = Column(String(32), nullable=True)


_engine = None
_SessionFactory: sessionmaker | None = None


def connect(db_path: str = "bot_state.sqlite3") -> None:
    global _engine, _SessionFactory
    _engine = create_engine(f"sqlite:///{db_path}")
    _SessionFactory = sessionmaker(bind=_engine)
    create_tables()


def create_tables() -> None:
    Base.metadata.create_all(_engine)


def close() -> None:
    global _engine, _SessionFactory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionFactory = None


@contextmanager
def _session() -> Session:
    if _SessionFactory is None:
        raise RuntimeError("db.connect() must be called before using db.py")
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _row_to_record(row: Trade) -> TradeRecord:
    return TradeRecord(
        id=row.id, symbol=row.symbol, direction=row.direction, entry_time=row.entry_time,
        entry_price=row.entry_price, atr=row.atr, stop=row.stop, take1=row.take1, take2=row.take2,
        qty_full=row.qty_full, qty_remaining=row.qty_remaining, take1_done=bool(row.take1_done),
        breakeven_stop=row.breakeven_stop, status=row.status, exit_reason=row.exit_reason,
        pnl_usd=row.pnl_usd, closed_at=row.closed_at, entry_order_id=row.entry_order_id,
        stop_order_id=row.stop_order_id, take2_order_id=row.take2_order_id,
    )


def has_open_trade(symbol: str) -> bool:
    with _session() as s:
        return s.query(Trade).filter_by(symbol=symbol, status="open").first() is not None


def save_trade(record: TradeRecord) -> int:
    with _session() as s:
        row = Trade(
            symbol=record.symbol, direction=record.direction, entry_time=record.entry_time,
            entry_price=record.entry_price, atr=record.atr, stop=record.stop,
            take1=record.take1, take2=record.take2, qty_full=record.qty_full,
            qty_remaining=record.qty_remaining, take1_done=record.take1_done, status="open",
            entry_order_id=record.entry_order_id, stop_order_id=record.stop_order_id,
            take2_order_id=record.take2_order_id,
        )
        s.add(row)
        s.flush()
        return row.id


def get_open_trades() -> list[TradeRecord]:
    with _session() as s:
        rows = s.query(Trade).filter_by(status="open").all()
        return [_row_to_record(r) for r in rows]


def mark_take1_done(trade_id: int, breakeven_stop: float, qty_remaining: float,
                     new_stop_order_id: str | None, new_take2_order_id: str | None = None) -> None:
    with _session() as s:
        row = s.get(Trade, trade_id)
        row.take1_done = True
        row.breakeven_stop = breakeven_stop
        row.qty_remaining = qty_remaining
        row.stop_order_id = new_stop_order_id
        if new_take2_order_id is not None:
            row.take2_order_id = new_take2_order_id


def close_trade(trade_id: int, exit_reason: str, pnl_usd: float) -> None:
    with _session() as s:
        row = s.get(Trade, trade_id)
        row.status = "closed"
        row.exit_reason = exit_reason
        row.pnl_usd = pnl_usd
        row.closed_at = datetime.now(timezone.utc).isoformat()
