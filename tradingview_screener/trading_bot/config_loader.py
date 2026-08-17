"""
Turns environment variables and parsed CLI args into a BotConfig /
StrategyConfig. config.py defines what's valid; this is the only module
that reads os.environ or an argparse.Namespace.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from config import BotConfig, StrategyConfig


def load_api_credentials() -> tuple[str, str]:
    return (os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""))


def bot_config_from_args(args) -> BotConfig:
    """args: an argparse.Namespace from bot/main.py's parser (watchlist,
    interval, leverage, poll_interval_sec, live, mainnet, db_path,
    order_size_usd)."""
    watchlist = tuple(s.strip().upper() for s in args.watchlist.split(",") if s.strip())
    strategy = StrategyConfig(order_size_usd=args.order_size_usd)
    return BotConfig(
        watchlist=watchlist,
        interval=args.interval,
        leverage=args.leverage,
        poll_interval_sec=args.poll_interval_sec,
        dry_run=not args.live,
        mainnet=args.mainnet,
        db_path=args.db_path,
        strategy=strategy,
    )
