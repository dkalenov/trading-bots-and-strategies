import asyncio
import logging
from datetime import datetime, timezone, timedelta, time
import math
import pandas as pd

logging.basicConfig(level=logging.INFO)


async def wait_for_next_candle(timeframe: str):
    now = datetime.now(timezone.utc)
    if "h" in timeframe:
        tf_hours = int(timeframe[:-1])
        next_hour = (now.hour // tf_hours + 1) * tf_hours
        next_time = now.replace(minute=0, second=5, microsecond=0, hour=next_hour % 24)
        if next_hour >= 24:
            next_time += timedelta(days=1)
    elif "m" in timeframe:
        tf_minutes = int(timeframe[:-1])
        next_minute = (now.minute // tf_minutes + 1) * tf_minutes
        next_time = now.replace(second=5, microsecond=0) + timedelta(minutes=(next_minute - now.minute))
    else:
        raise ValueError(f"Неподдерживаемый таймфрейм: {timeframe}")

    wait_seconds = max((next_time - now).total_seconds(), 0)
    logging.info(f"[{timeframe}] Ждем {int(wait_seconds)} секунд до закрытия свечи")
    await asyncio.sleep(wait_seconds)






def calculate_atr(klines: list, atr_length: int = 14) -> pd.DataFrame:
    df = pd.DataFrame(klines, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume",
        "CloseTime", "QuoteAssetVolume", "NumberOfTrades",
        "TakerBuyBase", "TakerBuyQuote", "Ignore"
    ])
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)

    if df.empty:
        return df

    df["H-L"] = df["High"] - df["Low"]
    df["H-C"] = abs(df["High"] - df["Close"].shift(1))
    df["L-C"] = abs(df["Low"] - df["Close"].shift(1))
    df["TR"] = df[["H-L", "H-C", "L-C"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(window=atr_length, min_periods=1).mean()
    return df.drop(columns=["H-L", "H-C", "L-C", "TR"])




# функция для округления вверх до нужного количества знаков
def round_up(num, decimals=0):
    multiplier = 10 ** decimals
    return math.ceil(num * multiplier) / multiplier


# функция для округления вниз до нужного количества знаков
def round_down(num, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(num * multiplier) / multiplier