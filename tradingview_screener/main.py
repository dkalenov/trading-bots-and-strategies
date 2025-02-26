import time
import requests
from datetime import datetime, timezone, timedelta
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL
from db import save_signal


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
VIETNAM_TZ = timezone(timedelta(hours=7))
IMPORTANT_SYMBOLS = {"BTCUSDT", "ETHUSDT"}  # BTC и ETH всегда сохраняем



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
    return [ticker['symbol'] for ticker in tickers if 'USDC' not in ticker['symbol'] and 'USDC' not in ticker['symbol'] and 'USDT' in ticker['symbol']]



def send_message(message):
    requests.get(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        params={"chat_id": TELEGRAM_CHANNEL, "text": message}
    )



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


def first_data():
    print('Search first data')
    send_message('Search first data')

    symbols = get_symbols()
    longs, shorts = [], []

    for symbol in symbols:
        try:
            data = get_data(symbol)
            signal = data.get("RECOMMENDATION", "NEUTRAL")
            entry_price = float(client.mark_price(symbol)["markPrice"])
            #

            if symbol in IMPORTANT_SYMBOLS or signal in {"STRONG_BUY", "STRONG_SELL"}:
                 save_signal(symbol, TIMEFRAME, signal, entry_price)

            if signal == 'STRONG_BUY':
                longs.append(symbol)
                print(f"{symbol}: {signal} at {entry_price}. Timeframe: {TIMEFRAME}")

            if symbol in IMPORTANT_SYMBOLS and signal not in {"STRONG_BUY", "STRONG_SELL"}:
                print(f"{symbol}: {signal} at {entry_price}. Timeframe: {TIMEFRAME}")

            elif signal == 'STRONG_SELL':
                shorts.append(symbol)
                print(f"{symbol}: {signal} at {entry_price}. Timeframe: {TIMEFRAME}")

                time.sleep(0.1)
        except Exception as e:
            # print(f"Error fetching {symbol}: {e}")
            time.sleep(0.01)


    print('Longs:', longs)
    print('Shorts:', shorts)
    return longs, shorts



print('START')
# wait_for_next_candle()
send_message('START')
longs, shorts = first_data()  # Заполняем начальные списки сигналов

while True:
    wait_for_next_candle()
    print('_______________________NEW ROUND_______________________')

    symbols = get_symbols()  # Обновляем список монет
    for symbol in symbols:
        try:
            data = get_data(symbol)
            signal = data.get("RECOMMENDATION", "NEUTRAL")
            entry_price = float(client.mark_price(symbol)["markPrice"])

            if symbol in IMPORTANT_SYMBOLS or signal in {"STRONG_BUY", "STRONG_SELL"}:
                save_signal(symbol, TIMEFRAME, signal, entry_price)
                if signal in {"BUY", "SELL", 'NEUTRAL'}:
                    print(f"{symbol}: {signal} at {entry_price}. Timeframe: {TIMEFRAME}")

            if signal == 'STRONG_BUY' and symbol not in longs:
                print(f"{symbol}: {signal} at {entry_price}. Timeframe: {TIMEFRAME}")
                send_message(symbol + ' BUY')
                longs.append(symbol)

            elif signal == 'STRONG_SELL' and symbol not in shorts:
                print(f"{symbol}: {signal} at {entry_price}. Timeframe: {TIMEFRAME}")
                send_message(symbol + ' SELL')
                shorts.append(symbol)

            time.sleep(0.1)
        except Exception as e:
            # print(f"Error fetching {symbol}: {e}")
            time.sleep(0.01)

