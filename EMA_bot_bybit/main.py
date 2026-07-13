import time
import numpy as np
from bybit import BybitApi
from indicators import ema

# ===== CONFIG =====
API_KEY = 'your_api_key'
API_SECRET = 'your_api_secret'
SYMBOL = 'ATOMUSDT'
QNT = 1
EMA_SHORT = 6
EMA_LONG = 12
TIMEFRAME = '5'
CANDLE_LIMIT = 50


def sleep_to_next_candle(interval_minutes=5):
    seconds = interval_minutes * 60 - time.time() % (interval_minutes * 60) + 2
    print(f'Sleeping for {seconds:.0f} seconds')
    time.sleep(seconds)


def get_open_position(client, symbol):
    """Check if there's an open position."""
    try:
        resp = client.http_request('GET', '/v5/position/list', {
            'category': 'linear',
            'symbol': symbol,
        })
        positions = resp[0]['result']['list']
        for pos in positions:
            if float(pos['size']) > 0:
                return pos['side'], float(pos['size'])
    except Exception as e:
        print(f'Error checking position: {e}')
    return None, 0


def close_position(client, symbol, side, size):
    """Close existing position."""
    close_side = 'Sell' if side == 'Buy' else 'Buy'
    return client.post_market_order(
        symbol=symbol,
        side=close_side,
        qnt=size,
        reduce_only=True
    )


if __name__ == '__main__':
    client = BybitApi(api_key=API_KEY, secret_key=API_SECRET, futures=True)

    while True:
        sleep_to_next_candle(interval_minutes=5)

        klines = client.get_klines(symbol=SYMBOL, interval=TIMEFRAME, limit=CANDLE_LIMIT)
        klines = klines['result']['list']

        # Remove incomplete candle (first one is the current unfinished)
        if len(klines) > 1:
            klines = klines[1:]

        numpy_klines = np.array(klines)
        close_prices = numpy_klines[:, 4].astype(float)

        ema_short = ema(close_prices, EMA_SHORT)
        ema_long = ema(close_prices, EMA_LONG)

        short_value = ema_short[-1]
        prev_short_value = ema_short[-2]
        long_value = ema_long[-1]
        prev_long_value = ema_long[-2]

        # Check current position
        pos_side, pos_size = get_open_position(client, SYMBOL)

        # Golden cross — open LONG (or flip from SHORT)
        if short_value > long_value and prev_short_value < prev_long_value:
            if pos_side == 'Sell' and pos_size > 0:
                print('Closing SHORT, then opening LONG')
                close_position(client, SYMBOL, 'Sell', pos_size)
                time.sleep(1)
            elif pos_side == 'Buy':
                print('Already LONG, skip')
                continue
            print('LONG! BUY!')
            client.post_market_order(symbol=SYMBOL, side='Buy', qnt=QNT)

        # Death cross — open SHORT (or flip from LONG)
        elif short_value < long_value and prev_short_value > prev_long_value:
            if pos_side == 'Buy' and pos_size > 0:
                print('Closing LONG, then opening SHORT')
                close_position(client, SYMBOL, 'Buy', pos_size)
                time.sleep(1)
            elif pos_side == 'Sell':
                print('Already SHORT, skip')
                continue
            print('SHORT! SELL!')
            client.post_market_order(symbol=SYMBOL, side='Sell', qnt=QNT)

        else:
            print('NO SIGNAL')

        print(f'EMA({EMA_SHORT}): {short_value:.4f} | EMA({EMA_LONG}): {long_value:.4f}')
