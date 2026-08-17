"""
Fetches TradingView's own technical rating via the `tradingview_ta` package,
which talks to TradingView's real scanner endpoint. This is deliberately
NOT a local reimplementation of the rating formula - the old project's
attempt at that (indicator/final_manual_signals_get_data.py,
trading_bot/indicator_test.py get_technical_rating()) is exactly the kind
of thing that goes stale and silently disagrees with the real thing, by
the old project's own admission in its README.

If TradingView changes their site/API in a way that breaks tradingview_ta,
this will raise - it will not fall back to a guess.
"""
from __future__ import annotations
from dataclasses import dataclass
from tradingview_ta import TA_Handler, Interval

INTERVAL_MAP = {
    "1m": Interval.INTERVAL_1_MINUTE,
    "5m": Interval.INTERVAL_5_MINUTES,
    "15m": Interval.INTERVAL_15_MINUTES,
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
}


@dataclass(frozen=True)
class Rating:
    symbol: str
    recommendation: str   # STRONG_BUY / BUY / NEUTRAL / SELL / STRONG_SELL
    buy_count: int
    sell_count: int
    neutral_count: int


class SignalProvider:
    """Thin wrapper so trader.py depends on this interface, not on
    tradingview_ta directly - makes it trivial to substitute a fake in
    tests without any network access."""

    def __init__(self, exchange: str = "BINANCE", screener: str = "crypto"):
        self.exchange = exchange
        self.screener = screener

    def get_rating(self, symbol: str, interval: str = "4h") -> Rating:
        handler = TA_Handler(
            symbol=symbol,
            exchange=self.exchange,
            screener=self.screener,
            interval=INTERVAL_MAP[interval],
        )
        analysis = handler.get_analysis()
        summary = analysis.summary
        return Rating(
            symbol=symbol,
            recommendation=summary["RECOMMENDATION"],
            buy_count=summary.get("BUY", 0),
            sell_count=summary.get("SELL", 0),
            neutral_count=summary.get("NEUTRAL", 0),
        )


class FakeSignalProvider:
    """Test double: returns whatever you pre-load, no network call ever."""

    def __init__(self):
        self._ratings: dict[str, Rating] = {}

    def set(self, symbol: str, recommendation: str):
        self._ratings[symbol] = Rating(symbol, recommendation, 0, 0, 0)

    def get_rating(self, symbol: str, interval: str = "4h") -> Rating:
        if symbol not in self._ratings:
            raise KeyError(f"no fake rating set for {symbol}")
        return self._ratings[symbol]
