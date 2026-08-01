"""
Downloads historical OHLCV klines from Binance's public data repository
(https://data.binance.vision) and caches them locally as CSV so repeated
runs don't re-download.

No API key is required — this uses Binance's public historical data
archive, not the authenticated trading API.
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests

COLUMNS = ["date", "open", "high", "low", "close", "volume"]


def _month_range(start_date: str, end_date: str) -> list[str]:
    """Return ['YYYY-MM', ...] from start_date to end_date, inclusive."""
    months = pd.period_range(start=start_date, end=end_date, freq="M")
    return [str(m) for m in months]


def download_klines(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    cache_dir: str = "klines",
    market: str = "futures/um",
) -> pd.DataFrame:
    """
    Download monthly kline archives and return one combined OHLCV
    DataFrame, sorted and de-duplicated, indexed by nothing (plain
    'date' column, UTC-aware).

    Parameters
    ----------
    symbol : e.g. "BTCUSDT"
    interval : e.g. "1h", "15m", "5m"
    start_date, end_date : "YYYY-MM" strings, inclusive
    cache_dir : folder for cached monthly .zip files
    market : "futures/um" (USDT-margined futures) or "spot"
    """
    os.makedirs(cache_dir, exist_ok=True)
    base_url = f"https://data.binance.vision/data/{market}/monthly/klines"

    rows: list[tuple] = []
    for month in _month_range(start_date, end_date):
        filename = f"{symbol}-{interval}-{month}.zip"
        file_path = os.path.join(cache_dir, filename)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            url = f"{base_url}/{symbol}/{interval}/{filename}"
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    f.write(resp.content)
            except requests.RequestException as e:
                print(f"[data_loader] skip {filename}: download failed ({e})")
                continue

        try:
            with zipfile.ZipFile(file_path) as zf:
                csv_name = f"{symbol}-{interval}-{month}.csv"
                with zf.open(csv_name) as fh:
                    reader = csv.reader(io.TextIOWrapper(fh, "utf-8"))
                    for row in reader:
                        if not row or not row[0].isdigit():
                            continue
                        rows.append(
                            (
                                datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc),
                                float(row[1]),
                                float(row[2]),
                                float(row[3]),
                                float(row[4]),
                                float(row[5]),
                            )
                        )
        except (zipfile.BadZipFile, KeyError) as e:
            print(f"[data_loader] skip corrupted archive {file_path}: {e}")
            continue

    if not rows:
        raise ValueError(
            f"No data could be downloaded for {symbol} {interval} "
            f"{start_date}..{end_date}. Check symbol/interval/date range."
        )

    df = pd.DataFrame(rows, columns=COLUMNS)
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    return df


def load_or_download(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    csv_path: str | None = None,
    cache_dir: str = "klines",
    market: str = "futures/um",
) -> pd.DataFrame:
    """Load klines from a local CSV cache, or download+cache them if missing."""
    if csv_path is None:
        csv_path = f"klines_{symbol}_{interval}_{start_date}_{end_date}.csv"

    if os.path.exists(csv_path):
        print(f"[data_loader] loading cached data: {csv_path}")
        return pd.read_csv(csv_path, parse_dates=["date"])

    print(f"[data_loader] downloading {symbol} {interval} {start_date}..{end_date} ...")
    df = download_klines(symbol, interval, start_date, end_date, cache_dir=cache_dir, market=market)
    df.to_csv(csv_path, index=False)
    print(f"[data_loader] saved {len(df)} rows -> {csv_path}")
    return df
