"""
UTBot Strategy — Database module.

Stores backtest results and optimization history in SQLite.
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class BacktestResult:
    id: Optional[int] = None
    symbol: str = ''
    interval: str = ''
    strategy_name: str = ''
    start_date: str = ''
    end_date: str = ''
    initial_capital: float = 0.0
    final_capital: float = 0.0
    total_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    profit_factor: float = 0.0
    avg_trade_duration_hours: float = 0.0
    commission_paid: float = 0.0
    params_json: str = '{}'
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class OptimizationResult:
    id: Optional[int] = None
    symbol: str = ''
    interval: str = ''
    strategy_name: str = ''
    start_date: str = ''
    end_date: str = ''
    total_evals: int = 0
    best_return_pct: float = 0.0
    best_params_json: str = '{}'
    best_sharpe: float = 0.0
    best_max_drawdown_pct: float = 0.0
    all_results_json: str = '[]'
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class Database:
    def __init__(self, db_path='utbot_results.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                interval TEXT,
                strategy_name TEXT,
                start_date TEXT,
                end_date TEXT,
                initial_capital REAL,
                final_capital REAL,
                total_return_pct REAL,
                max_drawdown_pct REAL,
                sharpe_ratio REAL,
                win_rate REAL,
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                avg_win_pct REAL,
                avg_loss_pct REAL,
                profit_factor REAL,
                avg_trade_duration_hours REAL,
                commission_paid REAL,
                params_json TEXT,
                created_at TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS optimization_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                interval TEXT,
                strategy_name TEXT,
                start_date TEXT,
                end_date TEXT,
                total_evals INTEGER,
                best_return_pct REAL,
                best_params_json TEXT,
                best_sharpe REAL,
                best_max_drawdown_pct REAL,
                all_results_json TEXT,
                created_at TEXT
            )
        ''')
        self.conn.commit()

    def save_backtest_result(self, result: BacktestResult) -> int:
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO backtest_results (
                symbol, interval, strategy_name, start_date, end_date,
                initial_capital, final_capital, total_return_pct, max_drawdown_pct,
                sharpe_ratio, win_rate, total_trades, winning_trades, losing_trades,
                avg_win_pct, avg_loss_pct, profit_factor, avg_trade_duration_hours,
                commission_paid, params_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            result.symbol, result.interval, result.strategy_name,
            result.start_date, result.end_date, result.initial_capital,
            result.final_capital, result.total_return_pct, result.max_drawdown_pct,
            result.sharpe_ratio, result.win_rate, result.total_trades,
            result.winning_trades, result.losing_trades, result.avg_win_pct,
            result.avg_loss_pct, result.profit_factor, result.avg_trade_duration_hours,
            result.commission_paid, result.params_json, result.created_at
        ))
        self.conn.commit()
        return cursor.lastrowid

    def save_optimization_result(self, result: OptimizationResult) -> int:
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO optimization_results (
                symbol, interval, strategy_name, start_date, end_date,
                total_evals, best_return_pct, best_params_json, best_sharpe,
                best_max_drawdown_pct, all_results_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            result.symbol, result.interval, result.strategy_name,
            result.start_date, result.end_date, result.total_evals,
            result.best_return_pct, result.best_params_json, result.best_sharpe,
            result.best_max_drawdown_pct, result.all_results_json, result.created_at
        ))
        self.conn.commit()
        return cursor.lastrowid

    def get_backtest_results(self, symbol=None, strategy_name=None, limit=50):
        cursor = self.conn.cursor()
        query = 'SELECT * FROM backtest_results'
        conditions = []
        params = []
        if symbol:
            conditions.append('symbol = ?')
            params.append(symbol)
        if strategy_name:
            conditions.append('strategy_name = ?')
            params.append(strategy_name)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        query += ' ORDER BY created_at DESC LIMIT ?'
        params.append(limit)
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_optimization_results(self, symbol=None, limit=20):
        cursor = self.conn.cursor()
        query = 'SELECT * FROM optimization_results'
        params = []
        if symbol:
            query += ' WHERE symbol = ?'
            params.append(symbol)
        query += ' ORDER BY created_at DESC LIMIT ?'
        params.append(limit)
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def close(self):
        self.conn.close()
