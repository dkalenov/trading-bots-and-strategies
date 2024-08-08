from binance.client import Client
import pandas as pd
import time
from config import api_key, api_secret

client = Client(api_key, api_secret)

def top_coin():
    all_tickers = pd.DataFrame(client.get_ticker())
    usdt = all_tickers[all_tickers['symbol'].str.endswith('USDT')]
    work = usdt[~((usdt.symbol.str.ontains('UP')) | (usdt.symbol.str.ontains('DOWN')))]
    top_coin = work[work.priceChangePercent == work.priceChangePercent.max()]
    top_coin = top_coin['symbol'].values[0]
    return top_coin

def last_data(symbol, interval, lookback):
    frame = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback + 'min ago UTC'))
    frame = frame.iloc[:,:6]
    frame.columns = ['Time', 'Opne', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame

def strategy(by_amt, SL = 0.985, Target = 1.02, open_position=False):
    try:
        asset = top_coin()
        df = last_data(asset, '1m', '120') # 120 mins
    
    except:
        time.sleep(61)
        asset = top_coin()
        df = last_data(asset, '1m', '120') # 120 mins

        