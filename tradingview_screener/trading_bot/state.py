"""
Local trade state in a single SQLite file. The original project needed a
running Postgres server just to remember which symbols had open positions -
for something one person runs on one machine, that's a dependency with no
payoff. sqlite3 is in the Python standard library.
"""
from __future__ import annotations
import sqlite3
from contextlib import closing
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_time TEXT NOT NULL,
    entry_price REAL NOT NULL,
    atr REAL NOT NULL,
    stop REAL NOT NULL,
    take1 REAL NOT NULL,
    take2 REAL NOT NULL,
    qty_full REAL NOT NULL,
    qty_remaining REAL NOT NULL,
    take1_done INTEGER NOT NULL DEFAULT 0,
    breakeven_stop REAL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    exit_reason TEXT,
    pnl_usd REAL,
    closed_at TEXT,
    entry_order_id TEXT,
    stop_order_id TEXT,
    take2_order_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_status ON trades(symbol, status);
"""


@dataclass
class TradeRecord:
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
    status: str = "OPEN"
    exit_reason: str | None = None
    pnl_usd: float | None = None
    closed_at: str | None = None
    entry_order_id: str | None = None
    stop_order_id: str | None = None
    take2_order_id: str | None = None
    id: int | None = None


class TradeStore:
    def __init__(self, db_path: str = "bot_state.sqlite3"):
        self.db_path = db_path
        with closing(self._conn()) as conn:
            conn.executescript(SCHEMA)
            conn.commit()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def has_open_position(self, symbol: str) -> bool:
        with closing(self._conn()) as conn:
            row = conn.execute(
                "SELECT 1 FROM trades WHERE symbol=? AND status='OPEN' LIMIT 1", (symbol,)
            ).fetchone()
            return row is not None

    def open_trade(self, t: TradeRecord) -> int:
        with closing(self._conn()) as conn:
            cur = conn.execute(
                """INSERT INTO trades
                   (symbol, direction, entry_time, entry_price, atr, stop, take1, take2,
                    qty_full, qty_remaining, take1_done, status,
                    entry_order_id, stop_order_id, take2_order_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t.symbol, t.direction, t.entry_time, t.entry_price, t.atr, t.stop,
                 t.take1, t.take2, t.qty_full, t.qty_remaining, int(t.take1_done), "OPEN",
                 t.entry_order_id, t.stop_order_id, t.take2_order_id),
            )
            conn.commit()
            return cur.lastrowid

    def get_open_trades(self) -> list[TradeRecord]:
        with closing(self._conn()) as conn:
            rows = conn.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()
            return [self._row_to_record(r) for r in rows]

    def mark_take1_done(self, trade_id: int, breakeven_stop: float,
                         qty_remaining: float, new_stop_order_id: str | None):
        with closing(self._conn()) as conn:
            conn.execute(
                """UPDATE trades SET take1_done=1, breakeven_stop=?, qty_remaining=?,
                   stop_order_id=? WHERE id=?""",
                (breakeven_stop, qty_remaining, new_stop_order_id, trade_id),
            )
            conn.commit()

    def close_trade(self, trade_id: int, exit_reason: str, pnl_usd: float):
        with closing(self._conn()) as conn:
            conn.execute(
                """UPDATE trades SET status='CLOSED', exit_reason=?, pnl_usd=?, closed_at=?
                   WHERE id=?""",
                (exit_reason, pnl_usd, datetime.now(timezone.utc).isoformat(), trade_id),
            )
            conn.commit()

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> TradeRecord:
        d = dict(row)
        d["take1_done"] = bool(d["take1_done"])
        return TradeRecord(**d)
