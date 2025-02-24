import time
import requests
import datetime
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL

TIMEFRAME = "1h"
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

client = UMFutures()


def get_data(symbol):
    output = TA_Handler(
        symbol=symbol,
        exchange='Binance',
        screener="crypto",
        interval=INTERVAL
    )
    activity = output.get_analysis().summary
    activity['SYMBOL'] = symbol
    return activity


def get_symbols():
    tickers = client.mark_price()
    return [ticker['symbol'] for ticker in tickers]


def send_message(message):
    requests.get(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        params={"chat_id": TELEGRAM_CHANNEL, "text": message}
    )


from datetime import datetime, timedelta, UTC
import time

from datetime import datetime, timedelta, timezone
import time

# Вьетнамский часовой пояс UTC+7
VIETNAM_TZ = timezone(timedelta(hours=7))



def wait_for_next_candle():
    now = datetime.now(VIETNAM_TZ) 

    if "h" in TIMEFRAME:
        tf_hours = int(TIMEFRAME[:-1])

        next_candle_hour = (now.hour // tf_hours + 1) * tf_hours
        next_candle_time = now.replace(hour=next_candle_hour % 24, minute=0, second=0, microsecond=0)


        if next_candle_hour >= 24:
            next_candle_time += timedelta(days=1)

    elif "m" in TIMEFRAME:
        tf_minutes = int(TIMEFRAME[:-1])
        next_candle_minute = (now.minute // tf_minutes + 1) * tf_minutes
        next_candle_time = now.replace(second=0, microsecond=0) + timedelta(minutes=(next_candle_minute - now.minute))

    if next_candle_time <= now:
        next_candle_time += timedelta(hours=tf_hours)

    wait_time = (next_candle_time - now).total_seconds()

    print(f"Now (Vietnam time): {now}")
    print(f"Next candle time (Vietnam time): {next_candle_time}")
    print(f"Waiting for next {TIMEFRAME} candle: {int(wait_time)} seconds ({int(wait_time // 60)} minutes)")

    if wait_time > 0:
        time.sleep(wait_time)


symbols = get_symbols()
longs, shorts = [], []


def first_data():
    print('Search first data')
    send_message('Search first data')
    for symbol in symbols:
        try:
            data = get_data(symbol)
            if data.get('RECOMMENDATION') == 'STRONG_BUY':
                longs.append(symbol)
            elif data.get('RECOMMENDATION') == 'STRONG_SELL':
                shorts.append(symbol)
            time.sleep(0.1)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            time.sleep(1)

    print('Longs:', longs)
    print('Shorts:', shorts)
    return longs, shorts


print('START')
send_message('START')
# wait_for_next_candle()
first_data()

while True:
    wait_for_next_candle()
    print('_______________________NEW ROUND_______________________')

    for symbol in symbols:
        try:
            data = get_data(symbol)
            print(data)
            if data.get('RECOMMENDATION') == 'STRONG_BUY' and symbol not in longs:
                print(symbol, 'BUY')
                send_message(symbol + ' BUY')
                longs.append(symbol)
            elif data.get('RECOMMENDATION') == 'STRONG_SELL' and symbol not in shorts:
                print(symbol, 'SELL')
                send_message(symbol + ' SELL')
                shorts.append(symbol)
            time.sleep(0.1)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            time.sleep(1)
