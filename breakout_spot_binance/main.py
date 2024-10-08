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

async def socket(symbol_list):
        global volume_dict

        url = "wss://stream.binance.com:9443/stream?streams="
        
        async with websockets.connect(url, ping_interval=None) as websocket:
            message = {
                    "method": "SUBSCRIBE",
                    "params": [f"{symbol.lower()}@kline_1d" for symbol in symbol_list],
                    "id": 1
                }
            print(message)
            await websocket.send(json.dumps(message))
            data = json.loads(await websocket.recv())
            async for message in websocket:
                data = json.loads(message)
                # print(data)

                symbol = data.get('data', {}).get('k', {}).get('s', None)
                close_price = float(data.get('data', {}).get('k', {}).get('c', None))
                volume = float(data.get('data', {}).get('k', {}).get('q', None))
                kline_close = data.get('data', {}).get('k', {}).get('x', None)

                if kline_close == True or volume_dict[symbol] == None:
                    volume_dict[symbol] = volume
                    print(volume_dict)
                compare_price_df(close_price, symbol, volume)

def compare_price_df(price, symbol, volume):
    global volume_dict
    high = extremum_dict[symbol]['high']
    low = extremum_dict[symbol]['low']
    try:
        delta_volume = int(volume / volume_dict[symbol])
    except:
        delta_volume = 0

    if (price <= 1.005*high and price >= high) and (delta_volume >= X):
        print(f"found situation on {symbol}. High is {high}")
        orders(price, symbol)
    elif (price >= 0.955*low and price <= low) and (delta_volume >= X):
        print("for futures trading")
    if price >= 1.005*high or price <= low*0.995:
        pass
        print('delete extremum and add new')

def cut_zeros(n):
    n = str(n)
    dec_part = n.split('0')
    return dec_part

def count(n):
    num_str = str(n)
    decimal_count = len(num_str.split('.')[1])
    return decimal_count

def getPrecision(symbol):
    spot_info = client.get_exchange_info()
    for x in range(0, len(spot_info['symbols'])):
        symbol_info = spot_info['symbols'][x]['symbol']
        if symbol_info == symbol:
            limit_precision = spot_info['symbols'][x]['filters'][0]['minPrice']
            order_ava = spot_info['symbols'][x]['orderTypes']
            print(order_ava)
            qty = spot_info['symbols'][x]['filters'][1]['minQty']
            limit_precision = cut_zeros(limit_precision)
            qty = cut_zeros(qty)
            qty = count(qty)
            limit_precision = count(limit_precision)
            return qty, limit_precision
        

def orders(price, symbol):
    qty, precision = getPrecision(symbol)
    print(qty, precision) 
    quantity = round(budget / price, qty)
    print(quantity)

    buy_order = client.create_order(symbol=symbol, 
                                    side='BUY', 
                                    type='MARKET', 
                                    quantity=quantity
                                    )
    
    print(buy_order)
    stopPrice = price * stop

    stop_loss = client.create_order(symbol=symbol, 
                                    side='SELL', 
                                    type='STOP_LOSS_LIMIT', 
                                    quantity=quantity,
                                    price=round(stopPrice, precision),
                                    stopPrice=round(stopPrice, precision),
                                    timeInForce='GTC'
                                    )
    print(stop_loss)
    binsocket(symbol, price, quantity)
    return

def binsocket(symbol, buy_price, quantity):
    socket = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange='binance.com')
    socket.create_stream(['kline_1d'], symbol, output='UnicornFy')
    while True:
        data = socket.pop_stream_data_from_stream_buffer()
        if data:
            try:
                price = float(data['kline']['close_price'])
                print(f"price:\t{price}")
                if price > buy_price:
                    buy_price = price
                trailing_stop = buy_price * trailing_stop_factor
                print(f"buy price:\t{buy_price}")
                print('****************************')
                if price < trailing_stop:
                    sell_order = client.create_order(symbol=symbol,
                                                    side='SELL',
                                                    type='MARKET',
                                                    quantity=quantity
                                                    )
                    print(sell_order)
                    return
            except:
                pass

if __name__ == "__main__":
    get_spot_list()
    get_futures_list()
    
    getted_list = [item for item in spot_list if item not in futures_list]

    sorted_list = sort_list(getted_list)
    
    get_klines(sorted_list)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(socket(sorted_list))

