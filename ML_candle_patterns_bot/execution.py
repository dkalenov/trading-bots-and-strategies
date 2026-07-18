"""Order execution — position lifecycle with SL/TP on exchange."""

import logging
import time
from dataclasses import dataclass
from exchange import BinanceFutures
from risk import RiskManager
from protection import ProtectionManager

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    symbol: str
    side: str
    entry_price: float
    quantity: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    timestamp: float


class ExecutionManager:
    def __init__(self, exchange: BinanceFutures, risk: RiskManager):
        self._exchange = exchange
        self._risk = risk
        self._protection = ProtectionManager(exchange)
        self._trades: dict[str, Trade] = {}
        self._open_count = 0

    def open_position(self, symbol: str, direction: int, atr: float) -> Trade | None:
        if not self._risk.can_open_new(self._open_count):
            return None
        if symbol in self._trades:
            return None

        try:
            entry_price = self._exchange.get_ticker_price(symbol)
        except Exception as e:
            logger.error("[EXEC] %s: Price failed: %s", symbol, e)
            return None

        symbol_info = self._exchange.get_exchange_info(symbol)
        sizing = self._risk.compute_position_size(symbol, direction, atr, entry_price, symbol_info)
        if sizing is None or float(sizing.quantity) <= 0:
            return None

        side = "BUY" if direction == 1 else "SELL"
        qty = float(sizing.quantity)

        try:
            self._exchange.set_leverage(symbol, self._risk.leverage)
            self._exchange.set_margin_type(symbol)
        except Exception:
            pass

        try:
            self._exchange.market_order(symbol, side, qty)
            logger.info("[EXEC] %s: %s %.4f @ %.4f", symbol, side, qty, entry_price)
        except Exception as e:
            logger.error("[EXEC] %s: Order failed: %s", symbol, e)
            return None

        time.sleep(0.5)

        stop_f = float(sizing.stop_price)
        tp1_f = float(sizing.take1_price)
        tp2_f = float(sizing.take2_price)

        # Place SL/TP on exchange
        self._protection.place_protection(symbol, side, qty, stop_f, tp1_f, tp2_f, 0.25)

        trade = Trade(
            symbol=symbol, side=side, entry_price=entry_price,
            quantity=qty, stop_price=stop_f,
            tp1_price=tp1_f, tp2_price=tp2_f, timestamp=time.time(),
        )
        self._trades[symbol] = trade
        self._open_count += 1
        return trade

    def close_position(self, symbol: str) -> bool:
        if symbol not in self._trades:
            return False
        trade = self._trades[symbol]
        close_side = "SELL" if trade.side == "BUY" else "BUY"
        try:
            self._protection.cancel_protection(symbol)
            self._exchange.market_order(symbol, close_side, trade.quantity)
            logger.info("[EXEC] %s: Closed", symbol)
            del self._trades[symbol]
            self._open_count -= 1
            return True
        except Exception as e:
            logger.error("[EXEC] %s: Close failed: %s", symbol, e)
            return False

    def partial_close(self, symbol: str, qty: float) -> bool:
        if symbol not in self._trades:
            return False
        trade = self._trades[symbol]
        close_side = "SELL" if trade.side == "BUY" else "BUY"
        try:
            self._exchange.market_order(symbol, close_side, qty)
            trade.quantity = round(trade.quantity - qty, 4)
            logger.info("[EXEC] %s: Partial close %.4f, remaining %.4f", symbol, qty, trade.quantity)
            if trade.quantity <= 0:
                del self._trades[symbol]
                self._open_count -= 1
            return True
        except Exception as e:
            logger.error("[EXEC] %s: Partial close failed: %s", symbol, e)
            return False

    def get_trade(self, symbol: str) -> Trade | None:
        return self._trades.get(symbol)

    def get_open_count(self) -> int:
        return self._open_count

    @property
    def protection(self) -> ProtectionManager:
        return self._protection
