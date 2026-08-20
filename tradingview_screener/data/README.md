# Data

## `tradingview_signals_2025-03-02_2025-09-21.csv`

The real, logged output of the original bot's TradingView rating checks,
trimmed to the clean, steadily-polled window used for the backtest in the
main README (the original log runs from 2025-02-26, but the first few
days look like initial testing rather than steady 4h polling, so this
file starts 2025-03-02). 162,322 rows, 380 symbols, all 4h. Columns:
`symbol, signal, entry_price, timeframe, unix_timestamp, utc_time, month`.

This file cannot be regenerated for a different date range - it's a
recording of what TradingView actually said at the time, not something
computed from a formula. If you want to backtest a different period,
you'd need to either run `bot/signals.py` live for a while to build up
your own log, or accept the disclosed limitation in `docs/AUDIT.md` (C4)
and write your own local approximation of TradingView's rating, knowing
going in that it won't exactly match the real thing.

## Klines (not included)

The 4h OHLCV klines used against this signal file were supplied
separately and are not bundled here (the full file is tens of megabytes
and is exactly reproducible from Binance's own public data, so there's no
reason to duplicate it in this repo). `backtest/engine.py` expects a CSV
with columns `Date, Open, High, Low, Close, Volume, Symbol` - `Date` as an
ISO timestamp with timezone, one row per symbol per 4h candle. Binance's
own futures klines endpoint (`/fapi/v1/klines`) or the monthly archives at
data.binance.vision will get you this in the same shape with a small
reshape script.
