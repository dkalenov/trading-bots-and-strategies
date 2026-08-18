"""
Classifies Binance API errors so callers can decide whether retrying is
worth it. execution/order_manager.py uses this instead of retrying blind.
"""
from __future__ import annotations


class BinanceAPIError(RuntimeError):
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self.payload = payload
        self.code = str(payload.get("code", "")) if isinstance(payload, dict) else ""
        super().__init__(f"Binance API error {status_code}: {payload}")


# Error codes that are deterministic - retrying the exact same request
# will fail the exact same way, so there's no point retrying.
PERMANENT_CODES = frozenset({
    "-1111",  # precision over the maximum defined for this asset
    "-4014",  # price not increased by tick size
    "-4120",  # order type not supported for this endpoint (use Algo Order API)
    "-2019",  # margin is insufficient
    "-1102",  # mandatory parameter missing / malformed
    "-2022",  # reduceOnly order is rejected
})

AUTH_CODES = frozenset({"-2014", "-2015", "-1022"})  # bad api key / signature / permissions


def classify(exc: Exception) -> str:
    if isinstance(exc, BinanceAPIError):
        if exc.code in AUTH_CODES:
            return "auth"
        if exc.code in PERMANENT_CODES:
            return "permanent"
        return "transient"
    return "unknown"


def is_permanent(exc: Exception) -> bool:
    return classify(exc) == "permanent"


def is_auth_error(exc: Exception) -> bool:
    return classify(exc) == "auth"
