import time
import requests
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL

INTERVAL = Interval.INTERVAL_1_DAY
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
    symbols = []
    for ticker in tickers:
        symbols.append(ticker['symbol'])  
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
            if data.get('RECOMMENDATION') == 'STRONG BUY':
                longs.append(symbol)
            elif data.get('RECOMMENDATION') == 'STRONG SELL':
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

while True:
    print('_______________________NEW ROUND_______________________')
    for symbol in symbols:
        try:
            data = get_data(symbol)
            if (data.get('RECOMMENDATION') == 'STRONG BUY') and (symbol not in longs):
                print(symbol, 'BUY')
                message = symbol + ' BUY'
                send_message(message)
                longs.append(symbol)
            
            if (data.get('RECOMMENDATION') == 'STRONG SELL') and (symbol not in shorts):
                print(symbol, 'SELL')
                message = symbol + ' SELL'
                send_message(message)
                shorts.append(symbol)
            time.sleep(0.1)
        except:
            pass
    time.sleep(300)
