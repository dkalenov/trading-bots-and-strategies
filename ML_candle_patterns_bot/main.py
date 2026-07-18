"""ML Candle Patterns Bot — with exchange-based SL/TP."""

import os
import sys
import time
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path

from exchange import BinanceFutures, ExecutionMode
from risk import RiskManager
from execution import ExecutionManager
from strategy import CandlePatternsML
from patterns import detect_candlestick_patterns
from utils import acquire_lock, release_lock, sleep_to_next_candle, compute_atr, compute_daily_regime, interval_to_seconds
from db import TradeDB
from health import HealthMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("ml_candle_bot")

LOCK_PATH = str(Path(__file__).parent / "bot.lock")
_shutdown = False


def _signal_handler(sig, frame):
    global _shutdown
    logger.info("Shutdown signal received")
    _shutdown = True


def load_config():
    mode_str = os.environ.get("EXECUTION_MODE", "dry_run").lower()
    mode = ExecutionMode(mode_str) if mode_str in ("dry_run", "testnet", "live") else ExecutionMode.DRY_RUN
    return {
        "mode": mode,
        "api_key": os.environ.get("BINANCE_API_KEY", ""),
        "api_secret": os.environ.get("BINANCE_API_SECRET", ""),
        "symbol": os.environ.get("SYMBOL", "ADAUSDT"),
        "interval": os.environ.get("INTERVAL", "15m"),
        "leverage": int(os.environ.get("LEVERAGE", "10")),
        "risk_per_trade_pct": float(os.environ.get("RISK_PCT", "0.01")),
        "max_positions": int(os.environ.get("MAX_POSITIONS", "3")),
        "stop_atr": float(os.environ.get("STOP_ATR", "1.5")),
        "take1_atr": float(os.environ.get("TAKE1_ATR", "2.0")),
        "take2_atr": float(os.environ.get("TAKE2_ATR", "4.0")),
        "partial_pct": float(os.environ.get("PARTIAL_PCT", "0.25")),
        "candle_limit": int(os.environ.get("CANDLE_LIMIT", "100")),
        "train_ratio": float(os.environ.get("TRAIN_RATIO", "0.7")),
        "use_ema_filter": os.environ.get("USE_EMA_FILTER", "true").lower() == "true",
    }


def run_once(cfg, exchange, risk, executor, strategy, db, health):
    klines = exchange.get_klines(cfg["symbol"], cfg["interval"], cfg["candle_limit"])
    if len(klines) < 2:
        return

    closes = [k["close"] for k in klines]
    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]

    import pandas as pd
    df = pd.DataFrame(klines)
    df = detect_candlestick_patterns(df)

    if strategy.model is None:
        logger.info("Training ML model...")
        strategy.train(cfg["train_ratio"], df)
        logger.info("Model: Acc=%.3f F1=%.3f AUC=%.3f",
                     strategy.metrics["accuracy"], strategy.metrics["f1"], strategy.metrics["roc_auc"])

    signals = strategy.predict(df)
    direction = 1 if signals.iloc[-1] == 1 else -1
    atr = compute_atr(highs, lows, closes)
    health.record_signal()

    if cfg["use_ema_filter"]:
        regime = compute_daily_regime(closes, interval_to_seconds(cfg["interval"]))
        if regime in ("flat",) or (regime == "bull" and direction == -1) or (regime == "bear" and direction == 1):
            return

    trade = executor.get_trade(cfg["symbol"])
    if trade:
        if trade.side == "BUY" and direction == -1:
            executor.close_position(cfg["symbol"])
            db.log_close(cfg["symbol"], closes[-1], 0, 0)
            logger.info("[FLIP] %s: LONG->SHORT", cfg["symbol"])
        elif trade.side == "SELL" and direction == 1:
            executor.close_position(cfg["symbol"])
            db.log_close(cfg["symbol"], closes[-1], 0, 0)
            logger.info("[FLIP] %s: SHORT->LONG", cfg["symbol"])
        else:
            return

    risk.update_equity(exchange.get_balance())
    trade = executor.open_position(cfg["symbol"], direction, atr)
    if trade:
        db.log_open(cfg["symbol"], trade.side, trade.entry_price, trade.quantity,
                     trade.stop_price, trade.tp1_price, trade.tp2_price)
        health.record_trade()
        logger.info("[OPEN] %s: %s @ %.4f SL=%.2f TP1=%.2f TP2=%.2f qty=%.4f",
                     cfg["symbol"], trade.side, trade.entry_price,
                     trade.stop_price, trade.tp1_price, trade.tp2_price, trade.quantity)


def main():
    global _shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    cfg = load_config()
    mode = cfg["mode"]
    dry_run = mode == ExecutionMode.DRY_RUN
    testnet = mode == ExecutionMode.TESTNET

    if not acquire_lock(LOCK_PATH):
        logger.error("Another instance running")
        sys.exit(1)

    try:
        if not dry_run and (not cfg["api_key"] or not cfg["api_secret"]):
            logger.error("Set BINANCE_API_KEY/SECRET")
            sys.exit(1)

        exchange = BinanceFutures(cfg["api_key"], cfg["api_secret"], testnet=testnet, dry_run=dry_run)
        risk = RiskManager(
            equity=exchange.get_balance(),
            risk_per_trade_pct=cfg["risk_per_trade_pct"],
            leverage=cfg["leverage"],
            max_positions=cfg["max_positions"],
            stop_atr=cfg["stop_atr"],
            take1_atr=cfg["take1_atr"],
            take2_atr=cfg["take2_atr"],
        )
        executor = ExecutionManager(exchange, risk)
        strategy = CandlePatternsML()
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        db = TradeDB(run_id=run_id, mode=mode.value)
        health = HealthMonitor(heartbeat_file=str(Path(__file__).parent / "heartbeat.tmp"))
        db.log_run_start(cfg["symbol"], cfg["interval"])

        logger.info("=" * 60)
        logger.info("ML Candle Patterns Bot | %s | %s", mode.value.upper(), run_id)
        logger.info("Symbol: %s | Interval: %s | Leverage: %dx", cfg["symbol"], cfg["interval"], cfg["leverage"])
        logger.info("Equity: $%.2f | Risk: %.1f%%", risk.equity, cfg["risk_per_trade_pct"] * 100)
        logger.info("SL: %.1f ATR | TP1: %.1f ATR | TP2: %.1f ATR", cfg["stop_atr"], cfg["take1_atr"], cfg["take2_atr"])
        logger.info("=" * 60)

        while not _shutdown:
            try:
                sleep_to_next_candle(cfg["interval"])
                run_once(cfg, exchange, risk, executor, strategy, db, health)
                health.write_heartbeat({"run_id": run_id, "mode": mode.value})
            except Exception as e:
                logger.error("Cycle error: %s", e, exc_info=True)
                health.record_error()
                time.sleep(60)

        stats = db.get_stats()
        logger.info("Done: %d trades, %d wins, PnL=$%.2f", stats["total"], stats["wins"], stats["pnl"])
        db.log_run_stop(stats["total"], stats["wins"], stats["pnl"])
        db.close()
    finally:
        release_lock(LOCK_PATH)
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
