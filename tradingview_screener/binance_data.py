import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from binance.client import Client
from db import save_signal
import json
import pprint

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
client = Client()

def get_prices_binance(symbols):
    """ Получаем цены сразу для всех символов """
    try:
        prices_raw = client.futures_mark_price()
        logging.info(f"Получены цены с Binance: {len(prices_raw)} символов")
        # pprint.pprint(prices_raw[:5])

        prices = {p["symbol"]: float(p["markPrice"]) for p in prices_raw if p["symbol"] in symbols}
        # logging.info(f"Отфильтрованные цены: {prices}")
        return prices
    except Exception as e:
        logging.error(f"Ошибка получения цен с Binance: {e}")
        return {}

# Получаем символы с Binance
def get_binance_symbols():
    """ Получает список доступных символов с Binance (только USDT) """
    try:
        exchange_info = client.get_exchange_info()
        return [
            symbol['symbol'] for symbol in exchange_info['symbols']
            if 'USDC' not in symbol['symbol'] and symbol['symbol'].endswith('USDT')
        ]
    except Exception as e:
        logging.error(f"Ошибка получения списка символов: {e}")
        return []



# symbols = ['BTCUSDT', 'ETHUSDT']
# get_prices_binance(symbols)
# print(get_binance_symbols())
# print(get_binance_symbols())
