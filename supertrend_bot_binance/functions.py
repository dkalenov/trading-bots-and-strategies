import time
import asyncio
import binance
from config import *

API_KEY = API_KEY_TEST
SECRET_KEY = API_SECRET_TEST

# API_KEY = BINANCE_API_KEY
# SECRET_KEY = BINANCE_SECRET_KEY



def sleep_to_next_interval(interval_str):
    interval_map = {
        '1m': 60,
        '3m': 180,
        '5m': 300,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '2h': 7200,
        '4h': 14400
    }

    interval = interval_map.get(interval_str)
    if interval is None:
        raise ValueError(f"Unsupported interval format: {interval_str}")

    # Расчёт времени до следующего интервала
    time_to_sleep = interval - time.time() % interval + 1

    # Преобразование времени ожидания в часы, минуты и секунды
    hours = int(time_to_sleep // 3600)
    minutes = int((time_to_sleep % 3600) // 60)
    seconds = int(time_to_sleep % 60)

    # Форматирование строки с оставшимся временем
    time_left_str = f"{hours}h {minutes}m {seconds}s"
    print(f'Sleeping for {time_left_str} until next {interval_str} interval')
    time.sleep(time_to_sleep)






async def filter_symbols(all_symbols, ignor_list):
    symbols = []
    for symbol_data in all_symbols.values():
        if symbol_data.symbol in ignor_list:
            continue
        if symbol_data.status == 'TRADING' and symbol_data.quote_asset == 'USDT' and symbol_data.contract_type == 'PERPETUAL':
            symbols.append(symbol_data.symbol)

    return symbols


async def sleep_to_next_min():
    time_to_sleep = 60 - time.time() % 60 + 1
    print('sleep', time_to_sleep)
    await asyncio.sleep(time_to_sleep)


