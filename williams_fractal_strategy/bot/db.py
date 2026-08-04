"""
Persistence layer — SQLite, not PostgreSQL.

algofactory_bot (the reference framework this project's bot/ borrows
proven patterns from — closePosition-style stops, min_qty/min_notional
sizing, websocket reconnect handling) requires a running PostgreSQL
server. That's the right choice for a multi-strategy production
system, but it's exactly the kind of setup step this project is trying
to avoid: SQLite needs nothing installed or running — it's a single
file, built into Python's standard library — while still giving real
persistence (positions survive a restart) and a real trade history
(not just "what's open right now").

Schema:
  positions — the one row per symbol that's currently open (mirrors
              what the bot believes is live; reconciled against the
              exchange on every startup and periodically thereafter,
              see main.py's reconcile())
  trades    — append-only history of closed trades, for your own
              records / analysis. Nothing in the bot reads this back
              except get_trade_history() for reporting.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from models import PositionState, TradeRecord

_SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT PRIMARY KEY,
    direction INTEGER NOT NULL,
    entry_price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    stop_order_id INTEGER,
    take_order_id INTEGER,
    opened_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    direction INTEGER NOT NULL,
    entry_price TEXT NOT NULL,
    exit_price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    pnl TEXT NOT NULL,
    exit_reason TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path = "bot.db"):
        self._path = str(path)
        self._conn = sqlite3.connect(self._path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---- positions ----

    def get_position(self, symbol: str) -> PositionState | None:
        row = self._conn.execute("SELECT * FROM positions WHERE symbol = ?", (symbol,)).fetchone()
        if row is None:
            return None
        return PositionState(
            symbol=row["symbol"], direction=row["direction"], entry_price=row["entry_price"],
            quantity=row["quantity"], stop_order_id=row["stop_order_id"],
            take_order_id=row["take_order_id"], opened_at=row["opened_at"],
        )

    def set_position(self, p: PositionState) -> None:
        self._conn.execute(
            """INSERT INTO positions (symbol, direction, entry_price, quantity, stop_order_id,
                                       take_order_id, opened_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol) DO UPDATE SET
                 direction=excluded.direction, entry_price=excluded.entry_price,
                 quantity=excluded.quantity, stop_order_id=excluded.stop_order_id,
                 take_order_id=excluded.take_order_id, opened_at=excluded.opened_at""",
            (p.symbol, p.direction, p.entry_price, p.quantity, p.stop_order_id,
             p.take_order_id, p.opened_at),
        )
        self._conn.commit()

    def clear_position(self, symbol: str) -> None:
        self._conn.execute("DELETE FROM positions WHERE symbol = ?", (symbol,))
        self._conn.commit()

    def all_open_symbols(self) -> list[str]:
        rows = self._conn.execute("SELECT symbol FROM positions").fetchall()
        return [r["symbol"] for r in rows]

    def count_open_positions(self) -> int:
        return self._conn.execute("SELECT COUNT(*) AS n FROM positions").fetchone()["n"]

    # ---- trade history ----

    def record_trade(self, t: TradeRecord) -> None:
        self._conn.execute(
            """INSERT INTO trades (symbol, direction, entry_price, exit_price, quantity, pnl,
                                    exit_reason, opened_at, closed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (t.symbol, t.direction, t.entry_price, t.exit_price, t.quantity, t.pnl,
             t.exit_reason, t.opened_at, t.closed_at),
        )
        self._conn.commit()

    def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[dict]:
        if symbol:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE symbol = ? ORDER BY id DESC LIMIT ?", (symbol, limit)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
