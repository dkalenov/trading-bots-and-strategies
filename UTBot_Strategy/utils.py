"""
UTBot Strategy — Utilities module.

Data loading, CSV export, and helper functions.
"""

import os
import csv
import io
import math
import zipfile
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pytz import timezone


def generate_months(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, '%Y-%m')
    end = datetime.strptime(end_date, '%Y-%m')
    months = []
    while start <= end:
        months.append(start.strftime('%Y-%m'))
        start += relativedelta(months=1)
    return months


def download_klines(symbol: str, interval: str, start_date: str, end_date: str,
                    klines_dir: str = 'klines', quiet: bool = False) -> pd.DataFrame:
    months = generate_months(start_date, end_date)
    os.makedirs(klines_dir, exist_ok=True)

    klines = {'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': []}

    for month in months:
        filename = f"{symbol}-{interval}-{month}.zip"
        file_path = os.path.join(klines_dir, filename)

        if not os.path.exists(file_path) or os.path.getsize(file_path) < 100:
            url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{interval}/{filename}"
            try:
                r = requests.get(url, allow_redirects=True, timeout=30)
                if r.status_code == 200 and len(r.content) > 100:
                    with open(file_path, 'wb') as f:
                        f.write(r.content)
                else:
                    if not quiet:
                        print(f"Warning: HTTP {r.status_code} for {filename}, skipping.")
                    continue
            except Exception as e:
                if not quiet:
                    print(f"Error downloading {filename}: {e}. Skipping.")
                continue

        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                csv_name = f"{symbol}-{interval}-{month}.csv"
                with zf.open(csv_name, 'r') as csv_file:
                    reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
                    for row in reader:
                        if row[0].isdigit():
                            ts = int(row[0])
                            if ts > 1e15:
                                ts = ts / 1000
                            klines['Date'].append(
                                datetime.fromtimestamp(ts / 1000, tz=timezone('UTC'))
                            )
                            klines['Open'].append(float(row[1]))
                            klines['High'].append(float(row[2]))
                            klines['Low'].append(float(row[3]))
                            klines['Close'].append(float(row[4]))
                            klines['Volume'].append(float(row[5]))
        except (zipfile.BadZipFile, KeyError) as e:
            print(f"Error: {file_path} is corrupted or not a ZIP. Skipping. ({e})")
            continue

    if not klines['Date']:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.DataFrame(klines)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.drop_duplicates(subset='Date').sort_values('Date')
    df.set_index('Date', inplace=True)

    if len(df) > 1:
        deltas = df.index.to_series().diff().dropna()
        expected = deltas.mode()[0] if len(deltas) else None
        gaps = deltas[deltas != expected]
        if len(gaps) > 0 and not quiet:
            print(f"Warning: {len(gaps)} gap(s) detected in candle series (expected spacing: {expected})")

    return df


def export_trades_csv(trades: list[dict], filepath: str):
    if not trades:
        print("No trades to export.")
        return
    df = pd.DataFrame(trades)
    df.to_csv(filepath, index=False)
    print(f"Exported {len(trades)} trades to {filepath}")


def calculate_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int) -> np.ndarray:
    """Wilder's ATR from raw arrays."""
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1])
        )
    )
    atr = np.full(len(closes), np.nan)
    if len(tr) >= period:
        atr[period] = np.mean(tr[:period])
        for i in range(period + 1, len(closes)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i - 1]) / period
    return atr


def calculate_atr_from_df(df: pd.DataFrame, period: int) -> np.ndarray:
    """Wilder's ATR from a DataFrame with High/Low/Close columns."""
    return calculate_atr(df['High'].values, df['Low'].values, df['Close'].values, period)


def compute_position_size(equity: float, risk_pct: float, stop_distance: float,
                           entry_price: float, leverage: float,
                           min_qty: float = 0.0, max_qty: float = float('inf'),
                           min_notional: float = 0.0, qty_decimals: int = None) -> float:
    """
    Risk-based position sizing — the SINGLE formula used by backtester.py,
    live_testnet.py, testnet_once.py, and scan_signals.py. Previously each
    of those files had its own, mutually inconsistent version of this
    (different leverage caps, and one of them silently ignored the
    `leverage` argument entirely) — this is why that broke down.

    - risk_amount = equity * risk_pct        (dollars you accept losing if
                                               the stop is hit)
    - stop_distance = |entry_price - stop_loss_price| (caller computes this,
                                               e.g. atr * stop_atr_mult)
    - raw quantity = risk_amount / stop_distance
    - capped so notional never exceeds equity * leverage — leverage is the
      ONE governing cap on position size here. There is no separate hidden
      cap: if you configure leverage=10, the position sizer is allowed to
      use up to 10x equity in notional (subject to the risk-based quantity
      being smaller, which it usually is for reasonable stop distances).
    - optionally floored to the exchange's quantity step size and bumped
      up to the exchange's minimum notional/quantity, if provided.
    """
    if stop_distance is None or stop_distance <= 0 or entry_price <= 0:
        return 0.0

    risk_amount = equity * risk_pct
    qty_by_risk = risk_amount / stop_distance

    max_notional = equity * leverage
    qty_by_leverage = max_notional / entry_price if entry_price > 0 else 0.0

    qty = min(qty_by_risk, qty_by_leverage)

    if qty_decimals is not None:
        factor = 10 ** qty_decimals
        qty = math.floor(qty * factor) / factor

    if qty < min_qty:
        qty = min_qty
    if qty > max_qty:
        qty = max_qty

    if entry_price > 0 and qty * entry_price < min_notional:
        needed = min_notional / entry_price
        if qty_decimals is not None:
            factor = 10 ** qty_decimals
            needed = math.ceil(needed * factor) / factor
        qty = needed

    return max(qty, 0.0)


def format_pct(value: float) -> str:
    return f"{value:+.2f}%"


def format_currency(value: float) -> str:
    return f"${value:,.2f}"
