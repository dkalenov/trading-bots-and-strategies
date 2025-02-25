import time
import requests
from datetime import datetime, timezone, timedelta
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL
from db import save_signal


import requests
from datetime import datetime, timezone, timedelta
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL
from db import save_signal


TIMEFRAME = "4h"
INTERVAL_MAPPING = {
    "1m": Interval.INTERVAL_1_MINUTE,
    "5m": Interval.INTERVAL_5_MINUTES,
    "15m": Interval.INTERVAL_15_MINUTES,
    "30m": Interval.INTERVAL_30_MINUTES,
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
}
INTERVAL = INTERVAL_MAPPING.get(TIMEFRAME, Interval.INTERVAL_4_HOURS)


now_utc = datetime.now(timezone.utc) # UTC
# print(now_utc)

VIETNAM_TZ = timezone(timedelta(hours=7))
now_local = now_utc.astimezone(VIETNAM_TZ)
# print(now_local)

def wait_for_next_candle():

    now_utc = datetime.now(timezone.utc)  # UTC
    now_local = now_utc.astimezone(VIETNAM_TZ)  # Local time (Vietnam)

    if "h" in TIMEFRAME:
        tf_hours = int(TIMEFRAME[:-1])
        next_candle_hour = (now_utc.hour // tf_hours + 1) * tf_hours
        next_candle_time_utc = now_utc.replace(hour=next_candle_hour % 24, minute=0, second=0, microsecond=0)

        if next_candle_hour >= 24:
            next_candle_time_utc += timedelta(days=1)

    elif "m" in TIMEFRAME:
        tf_minutes = int(TIMEFRAME[:-1])
        next_candle_minute = (now_utc.minute // tf_minutes + 1) * tf_minutes
        next_candle_time_utc = now_utc.replace(second=0, microsecond=0) + timedelta(
            minutes=(next_candle_minute - now_utc.minute))

    next_candle_time_local = next_candle_time_utc.astimezone(VIETNAM_TZ)
    wait_time = (next_candle_time_utc - now_utc).total_seconds()

    print(f"Now UTC: {now_utc} (Local: {now_local})")
    print(f"Next candle UTC: {next_candle_time_utc} (Local: {next_candle_time_local})")
    print(f"Waiting for next {TIMEFRAME} candle: {int(wait_time)} seconds ({int(wait_time // 60)} minutes)")

    if wait_time > 0:
        time.sleep(wait_time)


wait_for_next_candle()