import logging
from threading import Thread

import config
from binance_bot import BinanceGridBot, make_client
from grid_strategy import GridConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# One GridConfig per symbol you want to trade. `tp_pct` / `stop_loss_pct`
# are in "% ROI on margin used" (see grid_strategy.py docstring), NOT a
# raw price-move percentage — with leverage=1 they're roughly the same,
# but they diverge fast as leverage increases.
#
# stop_loss_pct=None means "no stop-loss" (classic grid behaviour: a
# stuck position rides out the whole trend). The backtest results in
# README.md show this is risky over a long, strongly trending window —
# consider setting a stop_loss_pct before running this unattended.
SYMBOLS = [
    GridConfig(symbol="BTCUSDT", n_levels=10, proportion=1.5, volume=0.05,
               tp_pct=3.0, leverage=1, stop_loss_pct=None),
    GridConfig(symbol="ETHUSDT", n_levels=10, proportion=1.5, volume=0.09,
               tp_pct=3.0, leverage=1, stop_loss_pct=None),
]

TESTNET = True   # keep True until you've validated behaviour on testnet
POLL_INTERVAL = 5.0  # seconds between order-book/position checks per symbol


def run_symbol(cfg: GridConfig):
    client = make_client(config.api, config.api_secret, testnet=TESTNET)
    bot = BinanceGridBot(client, cfg, poll_interval=POLL_INTERVAL)
    bot.run()


if __name__ == "__main__":
    if not config.api or not config.api_secret:
        raise SystemExit(
            "BINANCE_API_KEY / BINANCE_API_SECRET are not set.\n"
            "Export them as environment variables before running the bot, e.g.:\n"
            "  export BINANCE_API_KEY=...\n"
            "  export BINANCE_API_SECRET=..."
        )

    threads = [Thread(target=run_symbol, args=(cfg,), daemon=True) for cfg in SYMBOLS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
