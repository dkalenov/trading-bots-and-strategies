"""
Bollinger Bands Strategy — Utilities module.

Data loading (Bybit V5 API), CSV export, and helper functions.
"""

import os
import csv
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone


INTERVAL_MS = {
    '1m': 60_000,
    '3m': 180_000,
    '5m': 300_000,
    '15m': 900_000,
    '30m': 1_800_000,
    '1h': 3_600_000,
    '2h': 7_200_000,
    '4h': 14_400_000,
    '6h': 21_600_000,
    '12h': 43_200_000,
    '1d': 86_400_000,
}

# Bybit V5 API uses different interval codes
BYBIT_INTERVAL_MAP = {
    '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
    '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
    '1d': 'D', '1w': 'W', '1M': 'M',
}


def download_klines_bybit(symbol: str, interval: str, start_date: str, end_date: str,
                           klines_dir: str = 'klines', quiet: bool = False) -> pd.DataFrame:
    """Download historical klines from Bybit V5 API with pagination."""
    os.makedirs(klines_dir, exist_ok=True)

    cache_file = os.path.join(klines_dir, f"{symbol}-{interval}-{start_date}-{end_date}.csv")
    if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
        if not quiet:
            print(f"Loading cached klines from {cache_file}")
        df = pd.read_csv(cache_file, parse_dates=['Date'])
        df.set_index('Date', inplace=True)
        return df

    interval_ms = INTERVAL_MS.get(interval)
    if interval_ms is None:
        raise ValueError(f"Unknown interval: {interval}. Supported: {list(INTERVAL_MS.keys())}")

    start_ts = int(datetime.strptime(start_date, '%Y-%m').replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, '%Y-%m').replace(tzinfo=timezone.utc).timestamp() * 1000)
    # Extend end to end of month
    end_dt = datetime.strptime(end_date, '%Y-%m')
    if end_dt.month == 12:
        end_dt = end_dt.replace(year=end_dt.year + 1, month=1)
    else:
        end_dt = end_dt.replace(month=end_dt.month + 1)
    end_ts = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    all_klines = []
    current_start = start_ts
    batch = 0

    while current_start < end_ts:
        batch += 1
        bybit_interval = BYBIT_INTERVAL_MAP.get(interval, interval)
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': bybit_interval,
            'start': str(current_start),
            'limit': '200',
        }

        try:
            resp = requests.get('https://api.bybit.com/v5/market/kline',
                                params=params, timeout=30)
            data = resp.json()

            if data.get('retCode') != 0:
                if not quiet:
                    print(f"Bybit API error: {data.get('retMsg', 'unknown')}")
                break

            klines = data.get('result', {}).get('list', [])
            if not klines:
                break

            for k in klines:
                # Bybit V5 format: [startTime, open, high, low, close, volume, turnover]
                ts = int(k[0])
                if ts > end_ts:
                    break
                all_klines.append({
                    'Date': datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    'Open': float(k[1]),
                    'High': float(k[2]),
                    'Low': float(k[3]),
                    'Close': float(k[4]),
                    'Volume': float(k[5]),
                })

            # Move to next page — Bybit V5 returns newest-first, so klines[0] is newest
            newest_ts = int(klines[0][0])
            current_start = newest_ts + interval_ms

            if not quiet and batch % 10 == 0:
                print(f"  Downloaded {len(all_klines)} klines so far...")

            time.sleep(0.1)  # Rate limit

        except Exception as e:
            if not quiet:
                print(f"Error downloading klines: {e}")
            break

    if not all_klines:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.DataFrame(all_klines)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')]

    # Cache to CSV
    df.to_csv(cache_file)
    if not quiet:
        print(f"Saved {len(df)} klines to {cache_file}")

    return df


def export_trades_csv(trades: list[dict], filepath: str):
    if not trades:
        print("No trades to export.")
        return
    df = pd.DataFrame(trades)
    df.to_csv(filepath, index=False)
    print(f"Exported {len(trades)} trades to {filepath}")


def format_pct(value: float) -> str:
    return f"{value:+.2f}%"


def format_currency(value: float) -> str:
    return f"${value:,.2f}"
