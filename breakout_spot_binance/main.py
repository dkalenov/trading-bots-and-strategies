
from config import api_key, secret_key
import requests
from binance.client import Client
import websockets
import pandas as pd
import asyncio
import time 
import json
import pprint
import unicorn_binance_websocket_api

X = 2 # how many times should the volume increase the average volume for us to trade
budget = 20 # $
stoploss = 0.5 # %

stop = 1 - stoploss/100
stoplong = 1 + stoploss/100

trailing_stop_factor = 0.995

spot_list = []
futures_list = []
volume_dict = {}
extremum_dict = {}

client = Client(api_key=api_key, api_secret=secret_key)

def get_spot_list():
    global spot_list
    spot_info = client.get_exchange_info()
    for x in range(0, len(spot_info['symbols'])):
        symbol = spot_info['symbols'][x]['symbol']
        is_spot = None
        try:
            is_spot = spot_info['symbols'][x]['permissions'][0]
        except:
            pass
        if is_spot == 'SPOT':
            spot_list.append(symbol)

            
def get_futures_list():
    global futures_list
    futures_info = client.futures_exchange_info()

    for x in range(0, len(futures_info['symbols'])):
        symbol = futures_info['symbols'][x]['symbol']
        try:
            is_futures = futures_info['symbols'][x]['contractType']
        except:
            pass
        if is_futures == 'PERPETUAL':
            futures_list.append(symbol)

def sort_list(list):
    df = pd.DataFrame(list, columns=['symbol'])
    df = df[~(df.symbol.str.contains('BULL|BEAR|BUSD|USDC'))]
    df = df[df.symbol.str.contains('USDT')]
    getted_list = df['symbol'].to_list()
    return getted_list

def get_klines(symbol_list):
    global volume_dict
    global extremum_dict
    df = pd.DataFrame(columns=[symbol_list], index=['high', 'low'])
    for symbol in symbol_list:
        kline = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1DAY, limit=1)
        df.loc['high', symbol] = float(kline[0][2]) # add high kline to the first row
        df.loc['low', symbol] = float(kline[0][3]) # add low kline to the second row
        volume_dict[symbol] = None

    df = df.T
    data_dict = df.to_dict()
    for symbol, value_dict in data_dict.items():
        for key in value_dict.keys():
            extremum_dict[key[0]] = {'high': data_dict['high'][key], 'low': data_dict['low'][key]}
    print(extremum_dict)

#sorted_list = sort_list(get_spot_list())
#get_klines(sorted_list)
