"""
Parses Binance's /fapi/v1/exchangeInfo filters into the shape the rest of
the project needs. One place, so a filter-parsing bug shows up once.
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from models import SymbolFilters


def parse_symbol_filters(exchange_info: dict, symbol: str) -> SymbolFilters:
    for s in exchange_info["symbols"]:
        if s["symbol"] == symbol:
            step = tick = min_notional = None
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                elif f["filterType"] == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", f.get("minNotional", 5.0)))
            return SymbolFilters(step_size=step or 0.001, tick_size=tick or 0.01,
                                  min_notional=min_notional or 5.0)
    raise ValueError(f"symbol {symbol} not found in exchangeInfo")
