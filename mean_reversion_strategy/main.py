import matplotlib.pyplot as plt
import numpy as np
from config import api, secret
from pybit.unified_trading import HTTP
from talib import abstract
import time


client = HTTP(
    testnet=False,
    api_key=api,
    api_secret=secret
)

symbol = 'NEARUSDT'
threshold_percentage = 0.5
interval = 60 #mins

# get K-lines
def get_klines(symbol, interval):
    klines = client.get_kline(category='linear', symbol=symbol, interval=interval) # linear - Futures / spot - spot
    klines = klines['result']['list']
    return klines

# get close data
def get_close_data(klines):
    close = [float(i[4]) for i in klines]
    close = close[::-1] # at Bybit the newest candle is the first but talib expects to get the first candle as the oldest one so we need to reverse the list (Binace is oldest to newest)
    close = np.array(close)
    return close

# SMA
def get_sma(close):
    SMA = abstract.Function('sma') 
    data = SMA(close, timeperiod=25) # 25 periods, we can add some more periods if needed
    return data



def show_data():
    plt.figure(figsize=(14, 7))

    # close prices plot
    plt.plot(close, label='Close Prices', color='blue')

    # SMA plot
    plt.plot(data, label='SMA25', color='red')
    #plt.plot(sma5, label='SMA5', color='green')

    plt.title(f'Close Price and SMA 25')
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend()
    plt.show()


while True:
    close = get_close_data(get_klines(symbol, interval))
    data = get_sma(close)
    last_close = close[-1]
    last_sma = data[-1]
    print(f"last_close: {last_close}, last_sma: {last_sma}")

    if last_sma is not None:
        deviation = abs((last_close - last_sma) / last_sma) * 100
        print(f"Diviation: {deviation}%")

        if deviation > threshold_percentage:
            if last_close > last_sma:
                print('Open SELL order(SHORT)')
                order = client.place_order(category='linear', symbol=symbol, side='Sell', orderType='Market', qty=2) # qty=2 number of contracts equal to 2 
                print(order)
                
                while True:
                    close = get_close_data(get_klines(symbol, interval))
                    data = get_sma(close)
                    last_close = close[-1]
                    last_sma = data[-1]

                    if last_close <= last_sma:
                        print('Open BUY order(CLOSE POSITION)')
                        order = client.place_order(category='linear', symbol=symbol, side='Buy', orderType='Market', qty=2) # qty=2 number of contracts equal to 2 
                        print(order)
                        break
                    else:
                        print('Watching the position')
                        print(f'last_close: {last_close}, last_sma: {last_sma}')
                        time.sleep(20)

            else:
                print('Open BUY order(LONG)')
                order = client.place_order(category='linear', symbol=symbol, side='Buy', orderType='Market', qty=2) # qty=2 number of contracts equal to 2       
                print(order)
                while True:
                    close = get_close_data(get_klines(symbol, interval))
                    data = get_sma(close)
                    last_close = close[-1]
                    last_sma = data[-1]
                    if last_close >= last_sma:
                        print("Open SELL order(CLOSE POSITION)")
                        order = client.place_order(category='linear', symbol=symbol, side='Sell', orderType='Market', qty=2) # qty=2 number of contracts equal to 2
                        print(order)
                        break
                    else:
                        print('Watching the position')
                        print(f'last_close: {last_close}, last_sma: {last_sma}')
                        time.sleep(20)     
                break
        else:
            print('Insufficient data to calculate SMA')
            print(f'Sleeping for {interval} minutes')
            time.sleep(interval * 60) # sleep for interval minutes
    
                    
