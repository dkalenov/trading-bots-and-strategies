import time
from time import sleep
import numpy as np

from bybit import BybitApi
from indicators import ema
from config import bybit_api_key, bybit_secret_key


symbol = 'ATOMUSDT'
qnt = 1

def sleep_to_next_min():
    time_to_sleep = 60 - time.time() % 60 + 2 # exactly 1 minute
    print(f'Sleeping for {time_to_sleep} seconds')
    time.sleep(time_to_sleep)


if __name__ == '__main__':
    client = BybitApi(api_key=bybit_api_key, secret_key=bybit_secret_key, futures=True)

    while True:
        sleep_to_next_min()
        klines = client.get_klines(symbol=symbol, interval='5', limit=20)
        klines = klines['result']['list']
        last_candle = klines[0]

        if time.time() < int(last_candle[0]):
            klines.pop()

        numpy_klines = np.array(klines)
        close_prices = numpy_klines[:, 4].astype(float)

        ema_short = ema(close_prices, 6)
        ema_long = ema(close_prices, 12)

        short_value = ema_short[-1]
        prev_short_value = ema_short[-2]

        long_value = ema_long[-1]
        prev_long_value = ema_long[-2]

        if short_value > long_value and prev_short_value < prev_long_value:
            print('LONG! BUY!')
            client.post_market_order(symbol=symbol, side='Buy', qnt=qnt)
        elif short_value < long_value and prev_short_value > prev_long_value:
            print('SHORT! SELL!')
            client.post_market_order(symbol=symbol, side='Sell', qnt=qnt)
        else:
            print('NO SIGNAL')

        print(f'EMA Short: {short_value}')
        print(f'EMA Long: {long_value}')