"""
Grid Trading Bot — live execution on Binance USD-M Futures.

Trades the exact same strategy that backtest.py simulates (see
grid_strategy.py for the shared math). Fixes vs. the original version:

  - Price is read from the FUTURES mark price endpoint
    (`futures_mark_price`), not the spot ticker (`get_symbol_ticker`) —
    the old code was pricing a futures grid off the *spot* market, which
    on testnet is a completely separate exchange (testnet.binance.vision
    vs testnet.binancefuture.com).
  - Position size is read as a float and compared numerically
    (`amt != 0`), not string-compared against the literal `"0.000"`,
    which silently breaks for any symbol whose API response uses a
    different decimal precision.
  - Position amount / margin is summed with `float(...)`, not
    `.iloc[index]` on a filtered frame's original (non-reset) index —
    the old code could raise IndexError or read the wrong row in
    hedge-mode accounts.
  - Grid entry orders and the take-profit order are tagged with distinct
    `newClientOrderId` prefixes so the bot always knows which resting
    order is which, instead of assuming "whatever is on the other side".
  - Leverage is explicitly set via `futures_change_leverage` at startup,
    instead of just trusting whatever the account happens to have.
  - Order price/quantity are rounded to the symbol's actual precision
    (from `futures_exchange_info`), instead of a fixed global decimal
    count.
  - The polling loop sleeps between iterations and backs off on errors,
    instead of hammering the REST API in a bare `while True`.
  - An optional stop-loss (`GridConfig.stop_loss_pct`) can force-close a
    position early — off by default, matching the backtester.
"""

import logging
import time

from binance.client import Client
from binance.enums import (
    SIDE_BUY, SIDE_SELL, ORDER_TYPE_LIMIT, TIME_IN_FORCE_GTC,
)

from grid_strategy import (
    GridConfig,
    generate_grid_levels,
    calculate_tp_price,
    calculate_stop_price,
)

logger = logging.getLogger("grid_bot")


class BinanceGridBot:
    def __init__(self, client: Client, cfg: GridConfig, poll_interval: float = 5.0):
        self.client = client
        self.cfg = cfg
        self.poll_interval = poll_interval
        self._client_id_prefix = f"grid{cfg.symbol[:8]}"

        self._apply_symbol_precision()
        self._ensure_leverage()

    # ---------------------------------------------------------------- setup
    def _apply_symbol_precision(self):
        """Pull the symbol's real price/quantity precision from exchange
        info instead of trusting a hardcoded decimal count."""
        try:
            info = self.client.futures_exchange_info()
            for s in info['symbols']:
                if s['symbol'] == self.cfg.symbol:
                    self.cfg.price_decimals = s['pricePrecision']
                    self.qty_precision = s['quantityPrecision']
                    return
            logger.warning(f"[{self.cfg.symbol}] not found in exchange info — keeping configured precision")
            self.qty_precision = 3
        except Exception as e:
            logger.warning(f"[{self.cfg.symbol}] could not fetch exchange info: {e} — keeping configured precision")
            self.qty_precision = 3

    def _ensure_leverage(self):
        try:
            self.client.futures_change_leverage(symbol=self.cfg.symbol, leverage=self.cfg.leverage)
        except Exception as e:
            logger.warning(f"[{self.cfg.symbol}] could not set leverage to {self.cfg.leverage}x: {e}")

    def _round_qty(self, qty: float) -> float:
        return round(qty, self.qty_precision)

    # ------------------------------------------------------------- market data
    def get_mark_price(self) -> float:
        """Futures mark price — NOT the spot ticker."""
        data = self.client.futures_mark_price(symbol=self.cfg.symbol)
        return float(data['markPrice'])

    def get_position(self):
        """Returns (signed_position_amt, avg_entry_price, leverage) using
        numeric comparisons only (robust across symbols/precisions and
        one-way or hedge account modes)."""
        rows = self.client.futures_position_information(symbol=self.cfg.symbol)
        total_amt = 0.0
        notional = 0.0
        leverage = self.cfg.leverage
        for row in rows:
            amt = float(row['positionAmt'])
            if amt == 0:
                continue
            total_amt += amt
            notional += amt * float(row['entryPrice'])
            try:
                leverage = int(float(row.get('leverage', leverage)))
            except (TypeError, ValueError):
                pass
        if total_amt == 0:
            return 0.0, 0.0, leverage
        return total_amt, notional / total_amt, leverage

    def get_open_orders(self):
        return self.client.futures_get_open_orders(symbol=self.cfg.symbol)

    # ------------------------------------------------------------- order mgmt
    def cancel_by_prefix(self, orders, prefix):
        """Cancel every resting order whose clientOrderId starts with `prefix`."""
        for o in orders:
            if o.get('clientOrderId', '').startswith(prefix):
                try:
                    self.client.futures_cancel_order(symbol=self.cfg.symbol, orderId=o['orderId'])
                except Exception as e:
                    logger.warning(f"[{self.cfg.symbol}] cancel {o['orderId']} failed: {e}")

    def cancel_all(self):
        try:
            self.client.futures_cancel_all_open_orders(symbol=self.cfg.symbol)
        except Exception as e:
            logger.warning(f"[{self.cfg.symbol}] cancel_all failed: {e}")

    def draw_grid(self, center_price: float):
        """Place N buy limit orders below and N sell limit orders above
        `center_price`, tagged so we can identify them later."""
        cfg = self.cfg
        buys, sells = generate_grid_levels(center_price, cfg)
        qty = self._round_qty(cfg.volume)
        for i, price in enumerate(buys):
            try:
                self.client.futures_create_order(
                    symbol=cfg.symbol, side=SIDE_BUY, type=ORDER_TYPE_LIMIT,
                    timeInForce=TIME_IN_FORCE_GTC, quantity=qty, price=price,
                    newClientOrderId=f"{self._client_id_prefix}B{i}",
                )
            except Exception as e:
                logger.warning(f"[{cfg.symbol}] place buy@{price} failed: {e}")
        for i, price in enumerate(sells):
            try:
                self.client.futures_create_order(
                    symbol=cfg.symbol, side=SIDE_SELL, type=ORDER_TYPE_LIMIT,
                    timeInForce=TIME_IN_FORCE_GTC, quantity=qty, price=price,
                    newClientOrderId=f"{self._client_id_prefix}S{i}",
                )
            except Exception as e:
                logger.warning(f"[{cfg.symbol}] place sell@{price} failed: {e}")
        logger.info(f"[{cfg.symbol}] grid drawn around {center_price}: "
                    f"{len(buys)} buys, {len(sells)} sells")

    def ensure_tp_order(self, direction: str, position_amt: float, avg_entry: float):
        """(Re)place the single take-profit order for the whole position,
        only if the target price has actually moved (avoids needless
        cancel/replace churn on every poll)."""
        cfg = self.cfg
        signed_amt = position_amt if direction == 'LONG' else -position_amt
        target_tp = calculate_tp_price(avg_entry, signed_amt, cfg)
        tp_side = SIDE_SELL if direction == 'LONG' else SIDE_BUY
        tp_id = f"{self._client_id_prefix}TP"

        open_orders = self.get_open_orders()
        existing_tp = next((o for o in open_orders if o.get('clientOrderId') == tp_id), None)

        if existing_tp is not None and abs(float(existing_tp['price']) - target_tp) < 10 ** -cfg.price_decimals:
            return  # already correct, nothing to do

        if existing_tp is not None:
            try:
                self.client.futures_cancel_order(symbol=cfg.symbol, orderId=existing_tp['orderId'])
            except Exception as e:
                logger.warning(f"[{cfg.symbol}] cancel stale TP failed: {e}")

        try:
            self.client.futures_create_order(
                symbol=cfg.symbol, side=tp_side, type=ORDER_TYPE_LIMIT,
                timeInForce=TIME_IN_FORCE_GTC, quantity=self._round_qty(abs(position_amt)),
                price=target_tp, reduceOnly=True, newClientOrderId=tp_id,
            )
            logger.info(f"[{cfg.symbol}] TP order placed: {tp_side} {abs(position_amt)} @ {target_tp}")
        except Exception as e:
            logger.warning(f"[{cfg.symbol}] place TP failed: {e}")

    def market_close(self, direction: str, qty: float):
        side = SIDE_SELL if direction == 'LONG' else SIDE_BUY
        try:
            self.client.futures_create_order(
                symbol=self.cfg.symbol, side=side, type='MARKET',
                quantity=self._round_qty(qty), reduceOnly=True,
            )
            logger.warning(f"[{self.cfg.symbol}] market-closed {direction} {qty}")
        except Exception as e:
            logger.error(f"[{self.cfg.symbol}] market close failed: {e}")

    # ------------------------------------------------------------------ loop
    def run_once(self):
        cfg = self.cfg
        position_amt, avg_entry, leverage = self.get_position()

        if position_amt == 0:
            open_orders = self.get_open_orders()
            buys = [o for o in open_orders if o['clientOrderId'].startswith(self._client_id_prefix + 'B')]
            sells = [o for o in open_orders if o['clientOrderId'].startswith(self._client_id_prefix + 'S')]
            is_fresh_grid = len(buys) == cfg.n_levels and len(sells) == cfg.n_levels
            if open_orders and not is_fresh_grid:
                # leftover single-side orders from a cycle that just closed
                # (the opposite side was already cancelled when direction
                # was established) — clear them before redrawing
                self.cancel_all()
                open_orders = []
            if not open_orders:
                mark_price = self.get_mark_price()
                self.draw_grid(mark_price)
            return

        direction = 'LONG' if position_amt > 0 else 'SHORT'
        # position_amt from get_position() is already signed (+long/-short) —
        # do NOT re-negate it here (that was a real bug: it canceled out the
        # sign for SHORT positions and computed the wrong stop-loss side).
        signed_amt = position_amt

        # Optional stop-loss — off unless cfg.stop_loss_pct is set
        sl_price = calculate_stop_price(avg_entry, signed_amt, cfg)
        if sl_price is not None:
            mark_price = self.get_mark_price()
            hit = (direction == 'LONG' and mark_price <= sl_price) or \
                  (direction == 'SHORT' and mark_price >= sl_price)
            if hit:
                logger.warning(f"[{cfg.symbol}] stop-loss hit (mark={mark_price}, sl={sl_price}) — closing")
                self.cancel_all()
                self.market_close(direction, abs(position_amt))
                return

        # cancel the opposite side's stale grid-entry orders (idempotent)
        open_orders = self.get_open_orders()
        stale_prefix = f"{self._client_id_prefix}{'S' if direction == 'LONG' else 'B'}"
        self.cancel_by_prefix(open_orders, stale_prefix)

        # keep the TP order in sync with the current average entry / size
        self.ensure_tp_order(direction, abs(position_amt), avg_entry)

    def run(self):
        logger.info(f"[{self.cfg.symbol}] starting grid bot: "
                    f"n_levels={self.cfg.n_levels} proportion={self.cfg.proportion}% "
                    f"tp={self.cfg.tp_pct}% leverage={self.cfg.leverage}x "
                    f"stop_loss={self.cfg.stop_loss_pct}")
        backoff = self.poll_interval
        while True:
            try:
                self.run_once()
                backoff = self.poll_interval
            except Exception as e:
                logger.error(f"[{self.cfg.symbol}] loop error: {e} — backing off {backoff:.0f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)  # exponential backoff, capped at 5 min
                continue
            time.sleep(self.poll_interval)


def make_client(api_key: str, api_secret: str, testnet: bool = True) -> Client:
    return Client(api_key, api_secret, testnet=testnet)
