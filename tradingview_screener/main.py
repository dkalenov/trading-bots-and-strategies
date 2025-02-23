import time
import requests
import datetime
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL

INTERVAL = Interval.INTERVAL_1_HOUR
client = UMFutures()

def get_data(symbol):
    output = TA_Handler(
        symbol=symbol,
        exchange='Binance',
        screener="crypto",
        interval=INTERVAL
    )
    print('OUTPUT',output.get_analysis().indicators)
    activity = output.get_analysis().summary
    activity['SYMBOL'] = symbol
    return activity

def get_symbols():
    tickers = client.mark_price()
    symbols = []
    for ticker in tickers:
        symbols.append(ticker['symbol'])
        # print(ticker)
    return symbols

def send_message(message):
    res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                       params={"chat_id": TELEGRAM_CHANNEL, "text": message})



symbols = get_symbols()
longs = []
shorts = []

def first_data():
    print('Search first data')
    send_message('Search first data')
    for symbol in symbols:
        try:
            data = get_data(symbol)
            print('FIRST', data)
            if data.get('RECOMMENDATION') == 'STRONG_BUY':
                longs.append(symbol)
                # print(data['SYMBOL'], 'BUY')
            elif data.get('RECOMMENDATION') == 'STRONG_SELL':
                shorts.append(symbol)
            time.sleep(0.01)
        except:
            pass

    print('longs:', longs)
    print('shorts:', shorts)
    return longs, shorts

print('START')
send_message('START')
first_data()


def wait_for_next_candle():
    now = datetime.datetime.utcnow()
    minutes_to_wait = 240 - (now.minute % 240) 
    seconds_to_wait = minutes_to_wait * 60 - now.second  
    print(f"Waiting for next candle: {seconds_to_wait} seconds")
    time.sleep(seconds_to_wait)


while True:
    wait_for_next_candle() 
    print('_______________________NEW ROUND_______________________')

    for symbol in symbols:
        try:
            data = get_data(symbol)
            # print("NEW", data)
            if data.get('RECOMMENDATION') == 'STRONG_BUY' and symbol not in longs:
                print(symbol, 'BUY')
                send_message(symbol + ' BUY')
                longs.append(symbol)

            if data.get('RECOMMENDATION') == 'STRONG_SELL' and symbol not in shorts:
                print(symbol, 'SELL')
                send_message(symbol + ' SELL')
                shorts.append(symbol)
            time.sleep(0.1) 
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            time.sleep(1)
