import talib
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from bybit import BybitApi
from config import bybit_api_key, bybit_secret_key


symbol = 'ATOMUSDT'
qty = 1

def sleep_to_next_min():
    time_to_sleep = 60 - time.time() % 60 + 2
    print('sleep', time_to_sleep)
    time.sleep(time_to_sleep)

def check_open_positions(client, symbol):
    endpoint = '/v2/private/position/list'
    method = 'GET'
    params = {
        'symbol': symbol,
        'category': client.category
    }
    response_json, _ = client.http_request(method=method, endpoint=endpoint, params=params)
    if response_json and 'result' in response_json:
        positions = response_json['result']
        if positions:
            for pos in positions:
                if pos['symbol'] == symbol and pos['size'] > 0:
                    return pos
    return None


if __name__ == '__main__':
    client = BybitApi(api_key=bybit_api_key, secret_key=bybit_secret_key, futures=True)
    while True:
        sleep_to_next_min()
        klines = client.get_klines(symbol=symbol, interval='5', limit=100)
        klines = klines['result']['list']
        close_prices = [float(kline[4]) for kline in klines]
        close_prices_np = np.array(close_prices)
        close_prices_np = close_prices_np[::-1]

        upper_band, middle_band, lower_band = talib.BBANDS(close_prices_np, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)

        bollinger_df = pd.DataFrame({
            'Close': close_prices_np,
            'Upper Band': upper_band,
            'Middle Band': middle_band,
            'Lower Band': lower_band
        })

#        print(bollinger_df.tail())
#        plt.figure(figsize=(14, 8))
#        plt.plot(close_prices_np, label='Close Prices', linewidth=2)
#        plt.plot(upper_band, label='Upper Band', linestyle='--')
#        plt.plot(middle_band, label='Middle Band', linestyle='--')
#        plt.plot(lower_band, label='Lower Band', linestyle='--')
#        plt.title(f'Bollinger Bands for {symbol}')
#        plt.legend()
#        plt.show()  


        price = bollinger_df.iloc[-1]['Close']
        ub = bollinger_df.iloc[-1]['Upper Band']
        lb = bollinger_df.iloc[-1]['Lower Band']
        print('Price:', price)
        print('Upper Band:', ub)
        print('Lower Band:', lb)

        position = check_open_positions(client, symbol)

        if price > ub:
            if position and position['side'] == 'Buy':
                print('Already in LONG position')
            else:
                print('Opening LONG position!')
                client.http_request(
                    method='POST',
                    endpoint='/v2/private/order/create',
                    params={
                        'symbol': symbol,
                        'side': 'Buy',
                        'order_type': 'Market',
                        'qty': qty,
                        'time_in_force': 'GoodTillCancel'
                    }
                )
        elif price < lb:
            if position and position['side'] == 'Sell':
                print('Already in SHORT position')
            else:
                print('Opening SHORT position!')
                client.http_request(
                    method='POST',
                    endpoint='/v2/private/order/create',
                    params={
                        'symbol': symbol,
                        'side': 'Sell',
                        'order_type': 'Market',
                        'qty': qty,
                        'time_in_force': 'GoodTillCancel'
                    }
                )
        else:
            print('NO SIGNAL')
