from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pandas as pd


BINANCE_VISION_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"


def _parse_start(value: str) -> datetime:
    if len(value) == 7:
        return datetime.strptime(value, "%Y-%m").replace(tzinfo=timezone.utc)
    if len(value) == 10:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    raise ValueError("start/end dates must be YYYY-MM or YYYY-MM-DD")


def _parse_end_exclusive(value: str) -> datetime:
    if len(value) == 7:
        dt = datetime.strptime(value, "%Y-%m").replace(tzinfo=timezone.utc)
        year = dt.year + (1 if dt.month == 12 else 0)
        month = 1 if dt.month == 12 else dt.month + 1
        return dt.replace(year=year, month=month, day=1)
    if len(value) == 10:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    raise ValueError("start/end dates must be YYYY-MM or YYYY-MM-DD")


def _month_starts(start: datetime, end_exclusive: datetime) -> list[datetime]:
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months: list[datetime] = []
    while current < end_exclusive:
        months.append(current)
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = current.replace(year=year, month=month, day=1)
    return months


def ensure_ohlcv_frame(data: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close"]
    if (
        isinstance(data.index, pd.DatetimeIndex)
        and data.attrs.get("_sloping_ohlcv_normalized") is True
        and all(column in data.columns for column in required)
    ):
        return data

    missing = [column for column in required if column not in data.columns]
    if missing:
        raise ValueError(f"OHLCV data is missing columns: {missing}")

    df = data.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        date_column = next((name for name in ("Date", "date", "timestamp", "Timestamp") if name in df.columns), None)
        if not date_column:
            raise ValueError("Data must have a DatetimeIndex or a Date/timestamp column")
        df[date_column] = pd.to_datetime(df[date_column], utc=True)
        df = df.set_index(date_column)
    else:
        df.index = pd.to_datetime(df.index, utc=True)

    df.index = df.index.tz_convert(None)
    df = df.sort_index()

    if "Volume" not in df.columns:
        df["Volume"] = 0.0

    columns = ["Open", "High", "Low", "Close", "Volume"]
    df = df[columns]
    for column in columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    if df.empty:
        raise ValueError("OHLCV data is empty after normalization")
    df.attrs["_sloping_ohlcv_normalized"] = True
    return df


def load_ohlcv_csv(path: str | Path) -> pd.DataFrame:
    return ensure_ohlcv_frame(pd.read_csv(path))


def download_binance_vision_klines(
    symbol: str,
    interval: str,
    start: str,
    end: str,
    cache_dir: str | Path = "sloping_backtest/cache",
) -> pd.DataFrame:
    start_dt = _parse_start(start)
    end_exclusive = _parse_end_exclusive(end)
    if end_exclusive <= start_dt:
        raise ValueError("end must be later than start")

    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # --- Parquet-кеш: если уже есть готовый DataFrame, загружаем мгновенно ---
    start_key = start_dt.strftime("%Y-%m")
    end_key = (end_exclusive - timedelta(days=1)).strftime("%Y-%m")
    parquet_name = f"{symbol}-{interval}-{start_key}_{end_key}.parquet"
    parquet_path = cache_path / parquet_name
    if parquet_path.exists() and parquet_path.stat().st_size > 0:
        df = pd.read_parquet(parquet_path)
        df.attrs["_sloping_ohlcv_normalized"] = True
        return df

    rows: list[tuple[datetime, float, float, float, float, float]] = []
    for month_start in _month_starts(start_dt, end_exclusive):
        month_key = month_start.strftime("%Y-%m")
        filename = f"{symbol}-{interval}-{month_key}.zip"
        local_zip = cache_path / filename
        remote_url = f"{BINANCE_VISION_BASE}/{symbol}/{interval}/{filename}"

        if not local_zip.exists() or local_zip.stat().st_size == 0:
            try:
                with urlopen(remote_url, timeout=60) as response:
                    local_zip.write_bytes(response.read())
            except HTTPError as exc:
                if exc.code == 404:
                    continue
                raise
            except URLError:
                raise

        try:
            with zipfile.ZipFile(local_zip, "r") as archive:
                csv_name = f"{symbol}-{interval}-{month_key}.csv"
                with archive.open(csv_name, "r") as raw_csv:
                    reader = csv.reader(io.TextIOWrapper(raw_csv, encoding="utf-8"))
                    for row in reader:
                        if not row or not row[0].isdigit():
                            continue
                        open_time = datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc)
                        if not (start_dt <= open_time < end_exclusive):
                            continue
                        rows.append(
                            (
                                open_time,
                                float(row[1]),
                                float(row[2]),
                                float(row[3]),
                                float(row[4]),
                                float(row[5]),
                            )
                        )
        except (KeyError, zipfile.BadZipFile):
            continue

    if not rows:
        raise ValueError(
            f"No Binance Vision klines found for {symbol} {interval} in range {start} -> {end}"
        )

    df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    result = ensure_ohlcv_frame(df)

    # Сохраняем в Parquet для быстрой загрузки
    result.to_parquet(parquet_path)
    return result
