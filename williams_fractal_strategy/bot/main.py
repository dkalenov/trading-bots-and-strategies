#!/usr/bin/env python3
"""
Williams Fractal breakout bot — entry point and orchestration.

On every closed candle: run strategy.check_signal() (the exact
backtested fractal logic), size the trade with risk.compute_sizing(),
place entry + stop + take-profit through whichever gateway was
configured (real or dry-run), track state in db.py (SQLite), and keep
it reconciled against the exchange.

Run:
    python3 main.py --once     # single signal pass, then exit (smoke test)
    python3 main.py            # continuous run until Ctrl-C / SIGTERM

See README.md for the three-step verification path (offline -> testnet
smoke test -> continuous run) before ever pointing this at a funded
account.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path

from config import BotConfig
from db import Database
from filters import SymbolFilters
from models import PositionState, TradeRecord
from strategy import check_signal
from risk import compute_sizing
from utils import setup_logging
from bars import BarWindow
from websocket import MarketDataStream
from user_stream import UserDataStream

logger = logging.getLogger(__name__)


class Bot:
    def __init__(self, config: BotConfig, gateway, db_path: str = "bot.db"):
        self.config = config
        self.gateway = gateway
        self.db = Database(db_path)
        self.filters: dict[str, SymbolFilters] = {}
        self.bar_windows: dict[str, BarWindow] = {s: BarWindow() for s in config.symbols}
        self._debug_fired = False
        self._market_stream: MarketDataStream | None = None
        self._user_stream: UserDataStream | None = None
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()

    # ---------------------------------------------------------------- setup

    async def start(self, once: bool = False) -> None:
        logger.info("starting bot: mode=%s symbols=%s interval=%s debug_mode=%s",
                    self.config.execution_mode, self.config.symbols, self.config.interval,
                    self.config.debug_mode)

        for symbol in self.config.symbols:
            self.filters[symbol] = await self.gateway.get_symbol_filters(symbol)
            try:
                await self.gateway.change_leverage(symbol, self.config.exchange_leverage)
            except Exception:
                logger.warning("could not set leverage for %s (continuing)", symbol, exc_info=True)

        await self._warmup()
        await self.reconcile()

        if once:
            logger.info("--once mode: running a single signal pass then exiting")
            for symbol in self.config.symbols:
                await self.on_bar_closed(symbol)
            return

        self._market_stream = MarketDataStream(
            self.config.ws_base_url, self.config.symbols, self.config.interval,
            self.bar_windows, self.on_bar_closed,
        )
        self._user_stream = UserDataStream(self.gateway, self.config.ws_base_url, self.on_user_event)

        self._tasks = [
            asyncio.create_task(self._market_stream.run(), name="market_stream"),
            asyncio.create_task(self._user_stream.run(), name="user_stream"),
            asyncio.create_task(self._reconcile_loop(), name="reconcile_loop"),
        ]
        logger.info("bot running — %d background tasks", len(self._tasks))
        await self._stopping.wait()

    async def stop(self) -> None:
        logger.info("stopping bot")
        self._stopping.set()
        if self._market_stream:
            self._market_stream.stop()
        if self._user_stream:
            self._user_stream.stop()
        for t in self._tasks:
            t.cancel()
        try:
            await self.gateway.close_listen_key()
        except Exception:
            pass
        self.db.close()

    async def _warmup(self) -> None:
        if self.config.execution_mode == "dry_run":
            logger.info("dry_run: skipping REST warmup (feed bars via bar_windows directly — see scripts/verify_live_pipeline.py)")
            return
        limit = max(self.config.warmup_bars + 10, 100)
        for symbol in self.config.symbols:
            klines = await self.gateway.klines(symbol, self.config.interval, limit=limit)
            self.bar_windows[symbol].seed_from_rest_klines(klines)
            logger.info("warmup: %s loaded %d bars", symbol, len(self.bar_windows[symbol]))

    # ------------------------------------------------------------- signals

    async def on_bar_closed(self, symbol: str) -> None:
        try:
            await self._on_bar_closed_inner(symbol)
        except Exception:
            logger.exception("error handling closed bar for %s (continuing)", symbol)

    async def _on_bar_closed_inner(self, symbol: str) -> None:
        if self.db.get_position(symbol) is not None:
            return  # already in a position for this symbol

        if self.db.count_open_positions() >= self.config.max_positions:
            logger.debug("max_positions reached (%d) — skipping %s", self.config.max_positions, symbol)
            return

        bars = self.bar_windows[symbol].bars()
        force_debug = self.config.debug_mode and not self._debug_fired
        signal = check_signal(
            bars, fractal_n=self.config.fractal_n, warmup_bars=self.config.warmup_bars,
            atr_period=self.config.atr_period, force_debug_signal=force_debug,
        )
        if not signal.ready:
            logger.debug("%s: %d/%d warmup bars — not ready", symbol, len(bars), self.config.warmup_bars)
            return
        if signal.direction == 0:
            return
        if signal.forced:
            self._debug_fired = True
            logger.info("[DEBUG] forcing a LONG signal on %s to verify the order pipeline", symbol)

        entry_price = Decimal(str(bars[-1]["close"]))
        filters = self.filters[symbol]
        equity = await self.gateway.usdt_equity()

        sizing = compute_sizing(
            direction=signal.direction, entry_price=entry_price, equity=equity, filters=filters,
            structure_stop=signal.structure_stop, atr_value=signal.atr_value,
            risk_per_trade=self.config.risk_per_trade, stop_mode=self.config.stop_mode,
            atr_multiplier=self.config.atr_multiplier, stop_pct=self.config.stop_pct,
            reward_risk_ratio=self.config.reward_risk_ratio, max_leverage=self.config.max_leverage,
            min_stop_pct=self.config.min_stop_pct, max_stop_pct=self.config.max_stop_pct,
            debug_mode=self.config.debug_mode,
        )
        if not sizing.accepted:
            logger.warning("%s: sizing rejected trade: %s", symbol, sizing.rejected_reason)
            return

        dir_name = "LONG" if signal.direction == 1 else "SHORT"
        logger.info(
            "%s%s signal on %s: entry=%.6g stop=%.6g take=%.6g qty=%s notional=%.2f",
            "[DEBUG] " if signal.forced else "", dir_name, symbol,
            float(entry_price), float(sizing.stop_price), float(sizing.take_price),
            sizing.quantity, float(sizing.notional),
        )
        await self._place_trade(symbol, signal.direction, sizing)

    async def _place_trade(self, symbol: str, direction: int, sizing) -> None:
        side = "BUY" if direction == 1 else "SELL"
        close_side = "SELL" if direction == 1 else "BUY"

        entry = await self.gateway.new_order(
            symbol=symbol, side=side, type="MARKET", quantity=str(sizing.quantity),
        )
        filled = await self.gateway.wait_for_order_fill(symbol, entry["orderId"], timeout=30.0)
        fill_price = filled.get("avgPrice") or filled.get("price") or entry.get("avgPrice") or entry.get("price")
        logger.info("ENTRY filled: %s %s qty=%s price=%s orderId=%s",
                    symbol, side, sizing.quantity, fill_price, entry.get("orderId"))

        stop_algo = await self.gateway.new_algo_order(
            symbol=symbol, side=close_side, type="STOP_MARKET",
            algoType="CONDITIONAL", triggerPrice=str(sizing.stop_price),
            quantity=str(sizing.quantity),
            workingType="MARK_PRICE", positionSide="BOTH",
        )
        take_algo = await self.gateway.new_algo_order(
            symbol=symbol, side=close_side, type="TAKE_PROFIT_MARKET",
            algoType="CONDITIONAL", triggerPrice=str(sizing.take_price),
            quantity=str(sizing.quantity),
            workingType="MARK_PRICE", positionSide="BOTH",
        )
        logger.info("PROTECTION placed: %s stop@%s (algoId=%s) take@%s (algoId=%s)",
                    symbol, sizing.stop_price, stop_algo.get("algoId"),
                    sizing.take_price, take_algo.get("algoId"))

        self.db.set_position(PositionState(
            symbol=symbol, direction=direction,
            entry_price=str(fill_price or sizing.stop_price), quantity=str(sizing.quantity),
            stop_order_id=None, take_order_id=None,
            stop_algo_id=stop_algo.get("algoId"), take_algo_id=take_algo.get("algoId"),
            opened_at=str(int(time.time())),
        ))

    # --------------------------------------------------------- user events

    async def on_user_event(self, msg: dict) -> None:
        if msg.get("e") != "ORDER_TRADE_UPDATE":
            return
        o = msg.get("o", {})
        symbol = o.get("s")
        status = o.get("X")
        order_type = o.get("o")
        if status != "FILLED" or symbol is None:
            return

        state = self.db.get_position(symbol)
        if state is None:
            return

        if order_type not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            return

        filled_order_id = str(o.get("i", ""))
        filled_algo_id = str(o.get("ai", ""))
        is_known_order = (
            filled_order_id in (str(state.stop_order_id), str(state.take_order_id)) or
            filled_algo_id in (str(state.stop_algo_id), str(state.take_algo_id))
        )
        if not is_known_order and (filled_order_id or filled_algo_id):
            return

        realized = o.get("rp", "0")
        exit_price = o.get("ap") or o.get("L") or state.entry_price
        logger.info("PROTECTION FILLED: %s %s realizedPnl=%s — position closed",
                    symbol, order_type, realized)
        self.db.record_trade(TradeRecord(
            symbol=symbol, direction=state.direction, entry_price=state.entry_price,
            exit_price=str(exit_price), quantity=state.quantity, pnl=str(realized),
            exit_reason=order_type, opened_at=state.opened_at, closed_at=str(int(time.time())),
        ))
        try:
            await self.gateway.cancel_all_open_orders(symbol)
        except Exception:
            logger.debug("cancel_all_open_orders after protection fill failed (likely already flat)")
        try:
            for algo_id in (state.stop_algo_id, state.take_algo_id):
                if algo_id:
                    await self.gateway.cancel_algo_order(symbol, algo_id)
        except Exception:
            logger.debug("cancel_algo_order after protection fill failed (likely already flat)")
        self.db.clear_position(symbol)

    # --------------------------------------------------------- reconciliation

    async def _reconcile_loop(self) -> None:
        while not self._stopping.is_set():
            await asyncio.sleep(self.config.poll_reconcile_seconds)
            try:
                await self.reconcile()
            except Exception:
                logger.exception("reconcile() failed (will retry next cycle)")

    async def reconcile(self) -> None:
        """Compare local state against the exchange's actual positions and
        fix drift — e.g. a stop/take fill event was missed while the bot
        was disconnected. Exchange state always wins."""
        for symbol in self.config.symbols:
            local = self.db.get_position(symbol)
            risk = await self.gateway.position_risk(symbol)
            amt = Decimal(str(risk[0].get("positionAmt", "0"))) if risk else Decimal("0")

            if amt == 0 and local is not None:
                logger.warning("%s: exchange shows FLAT but local state had an open position — "
                                "clearing local state (protection order likely filled while offline)", symbol)
                self.db.clear_position(symbol)
            elif amt != 0 and local is None:
                direction = 1 if amt > 0 else -1
                logger.warning("%s: exchange shows an OPEN position with no local state "
                                "(placed manually or db lost) — adopting it, but it has "
                                "NO bot-managed stop/take until you place them manually", symbol)
                self.db.set_position(PositionState(
                    symbol=symbol, direction=direction,
                    entry_price=str(risk[0].get("entryPrice", "0")), quantity=str(abs(amt)),
                ))


# ------------------------------------------------------------------- CLI


def parse_args():
    p = argparse.ArgumentParser(description="Williams Fractal breakout bot")
    p.add_argument("--once", action="store_true", help="run a single signal pass then exit (smoke test)")
    p.add_argument("--env-file", default=None, help="path to .env (default: bot/.env)")
    p.add_argument("--db-file", default="bot.db")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


async def _amain(args) -> None:
    env_file = Path(args.env_file) if args.env_file else Path(__file__).resolve().parent / ".env"
    config = BotConfig.from_env(env_file)

    if config.execution_mode == "live" and not args.once:
        logger.warning(
            "EXECUTION_MODE=live — this places REAL orders with REAL money. "
            "Make sure you've verified everything on testnet first."
        )

    if config.execution_mode == "dry_run":
        from dry_run_gateway import DryRunGateway
        gateway = DryRunGateway()
        async with gateway:
            logger.warning(
                "dry_run mode has no market data feed on its own — bars must be "
                "fed manually (see scripts/verify_live_pipeline.py). Use "
                "EXECUTION_MODE=testnet for a real, self-driving smoke test."
            )
            bot = Bot(config, gateway, db_path=args.db_file)
            await _run_with_shutdown(bot, args.once)
    else:
        from gateway import Gateway
        async with Gateway(config.api_key, config.api_secret, config.rest_base_url) as gw:
            bot = Bot(config, gw, db_path=args.db_file)
            await _run_with_shutdown(bot, args.once)


async def _run_with_shutdown(bot: Bot, once: bool) -> None:
    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()

    def _handle_signal():
        stop_requested.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # Windows

    run_task = asyncio.create_task(bot.start(once=once))

    if once:
        await run_task
        return

    stop_task = asyncio.create_task(stop_requested.wait())
    done, pending = await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    await bot.stop()
    for t in pending:
        t.cancel()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
