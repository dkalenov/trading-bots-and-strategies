"""
Orchestrates entries and exits. Every decision (signal -> direction,
entry -> stop/take1/take2, take1 -> breakeven) goes through core.strategy,
identical to the backtester. exchange, signals and store are injected so
this whole file can be exercised in tests with zero network calls -
see tests/test_trader.py.

Design choice, disclosed: the original bot watched every tick via
websocket for take1. This bot polls mark price on an interval you set
(poll_open_positions(), called from main.py's loop). Polling is slower to
react but far simpler to reason about and cannot silently feed the ATR
core a tick when it expects a candle close - the exact bug class the old
project had. If you want tick-level reaction, that's a deliberate
follow-up, not something to bolt on quietly.
"""
from __future__ import annotations
import logging
import sys
import os
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.atr import wilder_atr
from core.strategy import (
    decide_entry, compute_exit_levels, breakeven_stop_price, position_size,
    StrategyConfig, LONG, SHORT,
)
from bot.state import TradeStore, TradeRecord

log = logging.getLogger("trader")


def klines_to_df(raw_klines: list) -> pd.DataFrame:
    """raw_klines: Binance /fapi/v1/klines response (list of lists)."""
    df = pd.DataFrame(raw_klines, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume", "CloseTime",
        "QuoteVolume", "Trades", "TakerBuyBase", "TakerBuyQuote", "Ignore",
    ])
    for c in ("Open", "High", "Low", "Close", "Volume"):
        df[c] = df[c].astype(float)
    return df


class Trader:
    def __init__(self, exchange, signals, store: TradeStore, watchlist: list[str],
                 scfg: StrategyConfig = None, interval: str = "4h",
                 dry_run: bool = True, leverage: int = 20, btc_symbol: str = "BTCUSDT"):
        self.exchange = exchange
        self.signals = signals
        self.store = store
        self.watchlist = watchlist
        self.scfg = scfg or StrategyConfig()
        self.interval = interval
        self.dry_run = dry_run
        self.leverage = leverage
        self.btc_symbol = btc_symbol

    # ---- entries, called once per candle close ----

    def run_entry_cycle(self):
        btc_recommendation = self.signals.get_rating(self.btc_symbol, self.interval).recommendation
        for symbol in self.watchlist:
            try:
                self._try_enter(symbol, btc_recommendation)
            except Exception:
                log.exception("entry check failed for %s", symbol)

    def _try_enter(self, symbol: str, btc_recommendation: str):
        if self.store.has_open_position(symbol):
            return

        rating = self.signals.get_rating(symbol, self.interval)
        direction = decide_entry(rating.recommendation, btc_recommendation)
        if direction is None:
            return

        raw_klines = self.exchange.get_klines(symbol, self.interval, limit=self.scfg.atr_length + 5)
        df = klines_to_df(raw_klines)
        atr_series = wilder_atr(df, self.scfg.atr_length)
        atr = atr_series.iloc[-1]
        if pd.isna(atr):
            log.warning("%s: not enough kline history for ATR(%d) yet", symbol, self.scfg.atr_length)
            return

        entry_price = float(df["Close"].iloc[-1])
        levels = compute_exit_levels(direction, entry_price, float(atr), self.scfg)
        filters = self.exchange.get_symbol_filters(symbol)
        qty = position_size(self.scfg.order_size_usd, entry_price, filters.step_size, filters.min_notional)

        log.info("%s ENTRY %s @ %.6f atr=%.6f stop=%.6f take1=%.6f take2=%.6f qty=%.6f%s",
                  symbol, direction, entry_price, atr, levels.stop, levels.take1, levels.take2,
                  qty, " [DRY RUN]" if self.dry_run else "")

        entry_order_id = stop_order_id = take2_order_id = None
        if not self.dry_run:
            self.exchange.set_leverage(symbol, self.leverage)
            side = "BUY" if direction == LONG else "SELL"
            opp = "SELL" if direction == LONG else "BUY"
            entry_order = self.exchange.new_market_order(symbol, side, qty)
            entry_order_id = str(entry_order.get("orderId"))
            stop_order = self.exchange.new_stop_market_order(symbol, opp, levels.stop, close_position=True)
            stop_order_id = str(stop_order.get("orderId"))
            take2_order = self.exchange.new_limit_order(symbol, opp, levels.take2, qty, reduce_only=True)
            take2_order_id = str(take2_order.get("orderId"))

        self.store.open_trade(TradeRecord(
            symbol=symbol, direction=direction,
            entry_time=datetime.now(timezone.utc).isoformat(),
            entry_price=entry_price, atr=float(atr),
            stop=levels.stop, take1=levels.take1, take2=levels.take2,
            qty_full=qty, qty_remaining=qty,
            entry_order_id=entry_order_id, stop_order_id=stop_order_id,
            take2_order_id=take2_order_id,
        ))

    # ---- position monitoring, called on a short poll interval ----

    def poll_open_positions(self):
        for trade in self.store.get_open_trades():
            try:
                self._poll_one(trade)
            except Exception:
                log.exception("poll failed for open trade %s (%s)", trade.id, trade.symbol)

    def _poll_one(self, trade: TradeRecord):
        mark_price = self.exchange.get_mark_price(trade.symbol)
        long = trade.direction == LONG

        if not trade.take1_done:
            take1_touched = (mark_price >= trade.take1) if long else (mark_price <= trade.take1)
            stop_touched = (mark_price <= trade.stop) if long else (mark_price >= trade.stop)
            if stop_touched:
                self._close(trade, trade.stop, "STOP")
            elif take1_touched:
                self._handle_take1(trade)
            return

        stop_touched = (mark_price <= trade.breakeven_stop) if long else (mark_price >= trade.breakeven_stop)
        take2_touched = (mark_price >= trade.take2) if long else (mark_price <= trade.take2)
        if stop_touched:
            self._close(trade, trade.breakeven_stop, "BREAKEVEN_STOP")
        elif take2_touched:
            self._close(trade, trade.take2, "TAKE2")

    def _handle_take1(self, trade: TradeRecord):
        close_qty = trade.qty_full * self.scfg.take1_portion
        remaining = trade.qty_remaining - close_qty
        be_stop = breakeven_stop_price(trade.direction, trade.entry_price, self.scfg)
        new_stop_order_id = trade.stop_order_id

        if not self.dry_run:
            side = "SELL" if trade.direction == LONG else "BUY"
            self.exchange.new_market_order(trade.symbol, side, close_qty, reduce_only=True)
            if trade.stop_order_id:
                self.exchange.cancel_order(trade.symbol, trade.stop_order_id)
            new_stop = self.exchange.new_stop_market_order(
                trade.symbol, side, be_stop, close_position=False, quantity=remaining)
            new_stop_order_id = str(new_stop.get("orderId"))

        log.info("%s take1 hit @ %.6f -> closed %.6f, stop moved to breakeven %.6f%s",
                  trade.symbol, trade.take1, close_qty, be_stop, " [DRY RUN]" if self.dry_run else "")
        self.store.mark_take1_done(trade.id, be_stop, remaining, new_stop_order_id)

    def _close(self, trade: TradeRecord, exit_price: float, reason: str):
        long = trade.direction == LONG
        qty = trade.qty_remaining
        gross = (exit_price - trade.entry_price) * qty if long else (trade.entry_price - exit_price) * qty

        if not self.dry_run:
            side = "SELL" if trade.direction == LONG else "BUY"
            open_id = trade.take2_order_id if trade.take1_done else trade.stop_order_id
            # cancel whichever protective order didn't trigger this exit
            other_id = trade.stop_order_id if reason == "TAKE2" else trade.take2_order_id
            if other_id:
                try:
                    self.exchange.cancel_order(trade.symbol, other_id)
                except Exception:
                    log.warning("could not cancel leftover order %s for %s", other_id, trade.symbol)
            if reason in ("STOP",):
                pass  # closePosition=True stop already flattened the position on the exchange
            else:
                self.exchange.new_market_order(trade.symbol, side, qty, reduce_only=True)

        log.info("%s CLOSED via %s @ %.6f pnl=%.4f%s", trade.symbol, reason, exit_price, gross,
                  " [DRY RUN]" if self.dry_run else "")
        self.store.close_trade(trade.id, reason, gross)
