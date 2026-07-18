"""Trade logging to SQLite database."""

import sqlite3
import logging
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "trades.db"


def init_db(db_path: str = None) -> sqlite3.Connection:
    """Initialize SQLite database with trades table."""
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL,
            quantity REAL NOT NULL,
            stop_price REAL,
            tp1_price REAL,
            tp2_price REAL,
            pnl REAL,
            pnl_pct REAL,
            status TEXT NOT NULL DEFAULT 'open',
            mode TEXT NOT NULL DEFAULT 'dry_run',
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            score REAL,
            atr REAL,
            patterns TEXT,
            mode TEXT NOT NULL DEFAULT 'dry_run',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            started_at TEXT NOT NULL,
            stopped_at TEXT,
            total_trades INTEGER DEFAULT 0,
            winning_trades INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0
        )
    """)
    conn.commit()
    return conn


class TradeDB:
    """Trade logging interface."""

    def __init__(self, db_path: str = None, run_id: str = "", mode: str = "dry_run"):
        self._conn = init_db(db_path)
        self._run_id = run_id
        self._mode = mode

    def log_open(self, symbol: str, side: str, entry_price: float, quantity: float,
                 stop_price: float, tp1_price: float, tp2_price: float):
        """Log trade open."""
        self._conn.execute(
            """INSERT INTO trades (run_id, symbol, side, entry_price, quantity,
               stop_price, tp1_price, tp2_price, status, mode, opened_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)""",
            (self._run_id, symbol, side, entry_price, quantity,
             stop_price, tp1_price, tp2_price, self._mode,
             datetime.now(timezone.utc).isoformat()),
        )
        self._conn.commit()
        logger.info("[DB] Trade opened: %s %s @ %.4f", symbol, side, entry_price)

    def log_close(self, symbol: str, exit_price: float, pnl: float, pnl_pct: float):
        """Log trade close."""
        self._conn.execute(
            """UPDATE trades SET exit_price=?, pnl=?, pnl_pct=?, status='closed', closed_at=?
               WHERE run_id=? AND symbol=? AND status='open'""",
            (exit_price, pnl, pnl_pct, datetime.now(timezone.utc).isoformat(),
             self._run_id, symbol),
        )
        self._conn.commit()
        logger.info("[DB] Trade closed: %s PnL=%.2f%%", symbol, pnl_pct * 100)

    def log_signal(self, symbol: str, direction: str, score: float, atr: float, patterns: str):
        """Log signal."""
        self._conn.execute(
            """INSERT INTO signals (run_id, symbol, direction, score, atr, patterns, mode)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (self._run_id, symbol, direction, score, atr, patterns, self._mode),
        )
        self._conn.commit()

    def log_run_start(self, symbol: str, interval: str):
        """Log run start."""
        self._conn.execute(
            """INSERT INTO runs (run_id, mode, symbol, interval, started_at)
               VALUES (?, ?, ?, ?, ?)""",
            (self._run_id, self._mode, symbol, interval,
             datetime.now(timezone.utc).isoformat()),
        )
        self._conn.commit()

    def log_run_stop(self, total_trades: int = 0, winning_trades: int = 0, total_pnl: float = 0):
        """Log run stop."""
        self._conn.execute(
            """UPDATE runs SET stopped_at=?, total_trades=?, winning_trades=?, total_pnl=?
               WHERE run_id=?""",
            (datetime.now(timezone.utc).isoformat(), total_trades, winning_trades, total_pnl,
             self._run_id),
        )
        self._conn.commit()

    def get_open_trades(self, symbol: str = None) -> list[dict]:
        """Get open trades."""
        if symbol:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE run_id=? AND symbol=? AND status='open'",
                (self._run_id, symbol),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE run_id=? AND status='open'",
                (self._run_id,),
            ).fetchall()
        return [dict(zip([d[0] for d in self._conn.execute("SELECT * FROM trades LIMIT 0").description], r)) for r in rows]

    def get_stats(self) -> dict:
        """Get run statistics."""
        row = self._conn.execute(
            """SELECT COUNT(*), SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), SUM(pnl)
               FROM trades WHERE run_id=? AND status='closed'""",
            (self._run_id,),
        ).fetchone()
        return {
            "total": row[0] or 0,
            "wins": row[1] or 0,
            "pnl": row[2] or 0.0,
        }

    def close(self):
        self._conn.close()
