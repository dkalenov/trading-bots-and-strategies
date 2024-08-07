import talib
import time
from time import sleep
import numpy as np
import talib
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from bybit import BybitApi
from config import bybit_api_key, bybit_secret_key

symbol='ATOMUSDT'

def sleep_to_next_min():
    time_to_sleep = 60 - time.time() % 60 + 2
    print('sleep', time_to_sleep)
    time.sleep(time_to_sleep)

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

        bollinger_df = pd.DataFrame(
            {
                'Close': close_prices_np,
                'Upper Band': upper_band,
                'Middle Band': middle_band,
                'Lower Band': lower_band
            }
        )

        #print(bollinger_df.tail())
        # plt.figure(figsize=(14, 8))
        # plt.plot(close_prices_np, label='Close Prices', linewidth=2)
        # plt.plot(upper_band, label='Upper Band', linestyle='---')
        # plt.plot(middle_band, label='Middle Band', linestyle='---')
        # plt.plot(lower_band, label='Lower Band', linestyle='---')
        # plt.title(f'Bollinger Bands for {symbol}')
        # plt.legend()
        # plt.show()  

        price = bollinger_df.iloc[-1]['Close']
        ub = bollinger_df.iloc[-1]['Upper Band']
        lb = bollinger_df.iloc[-1]['Lower Band']
        print('Price:', price )
        print('Upper Band:', ub)
        print('Lower Band:', lb)

        if price > ub:
            print('SHORT!')
            client.post_market_order(symbol=symbol, side='sell', qty=1, order_type='Market')
        elif price < lb:
            print('LONG!')
            client.post_market_order(symbol=symbol, side='buy', qty=1, order_type='Market')
        else:
            print('NO SIGNAL')
