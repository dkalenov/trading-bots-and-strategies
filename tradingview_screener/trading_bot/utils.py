import asyncio
import logging
from datetime import datetime, timezone, timedelta, time
from tradingview_ta import TA_Handler, Interval


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



def is_tradingview_symbols_available(symbol: str) -> bool:
    try:
        handler = TA_Handler(
            symbol=symbol,
            exchange='Binance',
            screener='crypto',
            interval=Interval.INTERVAL_1_HOUR,
        )
        handler.get_analysis()
        return True
    except Exception:
        return False


