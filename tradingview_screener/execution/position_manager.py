"""
Manages position lifecycle: entry on candle close, monitoring open
positions for take1/stop/take2. Adapted from the reference
architecture's PositionManager, scoped to what one strategy on one
exchange actually needs - no per-symbol asyncio locks (this bot is
sync/polling, not concurrent), no gateway abstraction over multiple
exchanges (Binance only, for now).

Every trading decision goes through risk.RiskManager and
strategies/tradingview_screener.py - nothing here computes an ATR, a
stop price, or a signal itself. That split is what makes this whole
project's backtest and live paths share one implementation instead of
three disagreeing ones (see docs/AUDIT.md, the original bug this
architecture exists to prevent a repeat of).
"""
from __future__ import annotations
import logging
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import wilder_atr, klines_to_dataframe
from models import TradeRecord, Direction
from risk import RiskManager
from execution.order_manager import OrderManager
from execution.protection import ProtectionManager
import db

log = logging.getLogger("position_manager")


class PositionManager:
    def __init__(self, exchange, signals, risk: RiskManager, watchlist: list[str],
                 scfg, interval: str = "4h", dry_run: bool = True, leverage: int = 20,
                 btc_symbol: str = "BTCUSDT"):
        self.exchange = exchange
        self.signals = signals
        self.risk = risk
        self.watchlist = watchlist
        self.scfg = scfg
        self.interval = interval
        self.dry_run = dry_run
        self.leverage = leverage
        self.btc_symbol = btc_symbol
        self.orders = OrderManager(exchange, dry_run=dry_run)
        self.protection = ProtectionManager(self.orders, risk)

    # ---- entries, called once per candle close ----

    def run_entry_cycle(self) -> None:
        btc_rating = self.signals.get_rating(self.btc_symbol, self.interval)
        for symbol in self.watchlist:
            try:
                self._try_enter(symbol, btc_rating)
            except Exception:
                log.exception("entry check failed for %s", symbol)

    def _try_enter(self, symbol: str, btc_rating) -> None:
        if db.has_open_trade(symbol):
            return

        rating = self.signals.get_rating(symbol, self.interval)
        direction = self.signals.decide(rating, btc_rating)
        if direction is None:
            return

        raw_klines = self.exchange.get_klines(symbol, self.interval, limit=self.scfg.atr_length + 5)
        df = klines_to_dataframe(raw_klines)
        atr_series = wilder_atr(df, self.scfg.atr_length)
        atr = atr_series.iloc[-1]
        if pd.isna(atr):
            log.warning("%s: not enough kline history for ATR(%d) yet", symbol, self.scfg.atr_length)
            return

        entry_price = float(df["Close"].iloc[-1])
        filters = self.exchange.get_symbol_filters(symbol)
        sizing = self.risk.compute_position_size(entry_price, float(atr), direction, filters)

        log.info("%s ENTRY %s @ %s atr=%.6f stop=%s take1=%s take2=%s qty=%s%s",
                  symbol, direction, sizing.entry_price, atr, sizing.stop, sizing.take1,
                  sizing.take2, sizing.quantity, " [DRY RUN]" if self.dry_run else "")

        side = "BUY" if direction == Direction.LONG.value else "SELL"
        opp = "SELL" if direction == Direction.LONG.value else "BUY"
        qty = float(sizing.quantity)

        entry_order_id = stop_order_id = take2_order_id = None
        if not self.dry_run:
            self.exchange.set_leverage(symbol, self.leverage)
        entry_order_id = self.orders.place_entry_market(symbol, side, qty)
        stop_order_id = self.orders.place_stop_algo(symbol, opp, float(sizing.stop), close_position=True)
        take2_order_id = self.orders.place_take_limit(symbol, opp, float(sizing.take2), qty)
        self.protection.mark_open(symbol)

        from datetime import datetime, timezone
        record = TradeRecord(
            symbol=symbol, direction=direction, entry_time=datetime.now(timezone.utc).isoformat(),
            entry_price=float(sizing.entry_price), atr=float(atr),
            stop=float(sizing.stop), take1=float(sizing.take1), take2=float(sizing.take2),
            qty_full=qty, qty_remaining=qty,
            entry_order_id=entry_order_id, stop_order_id=stop_order_id, take2_order_id=take2_order_id,
        )
        db.save_trade(record)

    # ---- position monitoring, called on a short poll interval ----

    def poll_open_positions(self) -> None:
        for trade in db.get_open_trades():
            try:
                self._poll_one(trade)
            except Exception:
                log.exception("poll failed for open trade %s (%s)", trade.id, trade.symbol)

    def _poll_one(self, trade: TradeRecord) -> None:
        mark_price = self.exchange.get_mark_price(trade.symbol)
        long = trade.direction == Direction.LONG.value

        if not trade.take1_done:
            take1_touched = (mark_price >= trade.take1) if long else (mark_price <= trade.take1)
            stop_touched = (mark_price <= trade.stop) if long else (mark_price >= trade.stop)
            if stop_touched:
                self._close(trade, trade.stop, "STOP")
            elif take1_touched:
                result = self.protection.handle_take1(trade, self.scfg)
                db.mark_take1_done(trade.id, result["breakeven_stop"],
                                    result["qty_remaining"], result["stop_order_id"],
                                    result["take2_order_id"])
            return

        stop_touched = (mark_price <= trade.breakeven_stop) if long else (mark_price >= trade.breakeven_stop)
        take2_touched = (mark_price >= trade.take2) if long else (mark_price <= trade.take2)
        if stop_touched:
            self._close(trade, trade.breakeven_stop, "BREAKEVEN_STOP")
        elif take2_touched:
            self._close(trade, trade.take2, "TAKE2")

    def _close(self, trade: TradeRecord, exit_price: float, reason: str) -> None:
        long = trade.direction == Direction.LONG.value
        qty = trade.qty_remaining
        gross = (exit_price - trade.entry_price) * qty if long else (trade.entry_price - exit_price) * qty

        self.protection.release(trade, reason)
        if reason != "STOP":
            # closePosition=True on the original stop already flattened
            # the position on the exchange when STOP triggers; TAKE2 and
            # BREAKEVEN_STOP both leave a remainder that needs an
            # explicit reduce-only market close.
            side = "SELL" if long else "BUY"
            self.orders.place_reduce_market(trade.symbol, side, qty)

        log.info("%s CLOSED via %s @ %.6f pnl=%.4f%s", trade.symbol, reason, exit_price, gross,
                  " [DRY RUN]" if self.dry_run else "")
        db.close_trade(trade.id, reason, gross)
