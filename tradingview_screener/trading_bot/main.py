"""
Usage examples:

    # safe default: no orders are ever sent, just logs what it would do
    python main.py --watchlist BTCUSDT,ETHUSDT,SOLUSDT

    # actually places orders on Binance Futures TESTNET
    python main.py --watchlist BTCUSDT,ETHUSDT --live

    # actually places orders with REAL MONEY - both flags are required on
    # purpose, so this can never happen from a copy-pasted --live command
    python main.py --watchlist BTCUSDT --live --mainnet

Reads BINANCE_API_KEY / BINANCE_API_SECRET from the environment (see
.env.example). Position/trade state lives in bot_state.sqlite3 in the
working directory - delete that file to reset from scratch.
"""
from __future__ import annotations
import argparse
import logging
import sys
import time
from datetime import datetime, timezone, timedelta

from config import ConfigError
from config_loader import bot_config_from_args, load_api_credentials
from utils import INTERVAL_SECONDS, candle_open_time
from exchange.binance.futures import Futures
from strategies.tradingview_screener import TradingViewScreenerStrategy, DebugSignalProvider
from risk import RiskManager
from execution.position_manager import PositionManager
import db
import health


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="comma-separated symbols, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--interval", default="4h", choices=list(INTERVAL_SECONDS))
    ap.add_argument("--live", action="store_true", help="actually place orders (default: dry run only)")
    ap.add_argument("--mainnet", action="store_true", help="use real-money Binance, not testnet")
    ap.add_argument("--leverage", type=int, default=20)
    ap.add_argument("--order-size-usd", type=float, default=10.0)
    ap.add_argument("--poll-interval-sec", type=int, default=60,
                     help="how often to check open positions for take1/stop/take2")
    ap.add_argument("--db-path", default="bot_state.sqlite3")
    ap.add_argument("--once", action="store_true", help="run a single entry cycle and exit (for cron/testing)")
    ap.add_argument("--skip-health-check", action="store_true")
    ap.add_argument("--debug-entry", action="store_true",
                    help="force LONG entry for every symbol (proves Binance + TradingView connections)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("main")

    if args.mainnet and not args.live:
        log.error("--mainnet without --live doesn't make sense; refusing to start")
        sys.exit(1)

    try:
        cfg = bot_config_from_args(args)
    except ConfigError as e:
        log.error("invalid configuration: %s", e)
        sys.exit(1)

    if cfg.dry_run:
        log.warning("DRY RUN: no orders will be sent. Pass --live to actually trade (testnet by default).")
    elif not cfg.mainnet:
        log.warning("LIVE on Binance Futures TESTNET. Pass --mainnet as well for real money.")
    else:
        log.warning("LIVE on Binance Futures MAINNET. This trades real money.")

    api_key, api_secret = load_api_credentials()
    if args.live and not (api_key and api_secret):
        log.error("BINANCE_API_KEY / BINANCE_API_SECRET must be set to use --live")
        sys.exit(1)

    exchange = Futures(api_key, api_secret, testnet=not cfg.mainnet)
    real_signals = TradingViewScreenerStrategy()
    if args.debug_entry:
        log.warning("DEBUG ENTRY MODE: forcing LONG for every symbol, real TradingView ratings still fetched")
        btc_rating = real_signals.get_rating("BTCUSDT", cfg.interval)
        log.info("BTCUSDT rating (debug): %s", btc_rating)
        signals = DebugSignalProvider(real_signals, btc_rating)
    else:
        signals = real_signals
    risk = RiskManager(cfg.strategy)
    db.connect(cfg.db_path)

    if args.live and not args.skip_health_check:
        if not health.run_all(db, exchange):
            log.error("health check failed, refusing to start --live (use --skip-health-check to override)")
            sys.exit(1)

    position_manager = PositionManager(
        exchange, signals, risk, list(cfg.watchlist), cfg.strategy,
        interval=cfg.interval, dry_run=cfg.dry_run, leverage=cfg.leverage,
    )

    if args.once:
        position_manager.run_entry_cycle()
        position_manager.poll_open_positions()
        return

    interval_seconds = INTERVAL_SECONDS[cfg.interval]
    last_cycle_candle = None
    log.info("watching %s on %s, poll every %ss", cfg.watchlist, cfg.interval, cfg.poll_interval_sec)
    while True:
        now = datetime.now(timezone.utc)
        this_candle = candle_open_time(now, cfg.interval)
        settled = now - this_candle > timedelta(seconds=10)  # let TradingView's own candle settle
        if settled and this_candle != last_cycle_candle:
            position_manager.run_entry_cycle()
            last_cycle_candle = this_candle

        position_manager.poll_open_positions()
        time.sleep(cfg.poll_interval_sec)


if __name__ == "__main__":
    main()
