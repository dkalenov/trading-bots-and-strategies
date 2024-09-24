import asyncio
import time
import traceback
import binance
import numpy as np
from functions import *
from indicators import *
from config import *
import threading
import pprint as pp
import queue





API_KEY = API_KEY_TEST
SECRET_KEY = API_SECRET_TEST

# API_KEY = BINANCE_API_KEY
# SECRET_KEY = BINANCE_SECRET_KEY


shutdown_event = asyncio.Event()
filter_symbols_event = asyncio.Event()

all_symbols = {}
ignor_list = ['ETHFIUSDT', 'USDCUSDT']
timeframe = '1m'
klines_ws = []
historical_klines = {}
limit_klines = 50
order_pool = asyncio.Queue()
filtered_symbols = []
my_ws_userdata = None
positions = []
max_positions = 3
multiplier=1.2

# Пороговые значения для ATR и ADX
atr_threshold = 0.5  # Устанавливаете свой порог ATR
adx_threshold = 25  # Устанавливаете свой порог ADX




#semaphore = asyncio.Semaphore(10)

async def sleep_to_next_min():
    time_to_sleep = 60 - time.time() % 60 + 1
    print('sleep', time_to_sleep)
    await asyncio.sleep(time_to_sleep)

# async def check_volatility(symbol):
#     # Получаем данные о волатильности
#     klines = await client.klines(symbol=symbol, interval=timeframe, limit=limit_klines)
#     close_prices = [float(kline[4]) for kline in klines]
#     volatility = (max(close_prices) - min(close_prices)) / min(close_prices)
#
#     # Устанавливаем порог волатильности
#     if volatility < 0.01:  # Например, если волатильность меньше 1%
#         print(f"Low volatility for {symbol}. Skipping...")
#         return False
#     return True


# Устанавливаем ограничение на количество одновременных запросов (например, 10)


async def filter_symbols(all_symbols, ignor_list):
    # Сначала фильтруем символы по нужным параметрам (USDT и PERPETUAL)
    filtered_symbols = [
        symbol_data.symbol for symbol_data in all_symbols.values()
        if symbol_data.symbol not in ignor_list
        and symbol_data.status == 'TRADING'
        and symbol_data.quote_asset == 'USDT'
        and symbol_data.contract_type == 'PERPETUAL'
    ]

    # Создаем задачи для проверки ликвидности только для отфильтрованных символов
    tasks = {symbol: check_liquidity(symbol) for symbol in filtered_symbols}

    # Выполняем все задачи параллельно с ограничением на одновременное выполнение
    liquidity_results = await asyncio.gather(*tasks.values())

    # Фильтруем символы на основе результатов проверки ликвидности
    symbols = [symbol for symbol, result in zip(tasks.keys(), liquidity_results) if result]

    # Логируем, какие символы не прошли проверку ликвидности
    for symbol, result in zip(tasks.keys(), liquidity_results):
        if not result:
            print(f"Insufficient liquidity for {symbol}. Skipping...")

    return symbols


async def check_liquidity(symbol, min_liquidity=1000):
    limit = 100
    semaphore = asyncio.Semaphore(limit)
    async with semaphore:
        try:
            # Получаем книгу ордеров для символа через метод depth
            order_book = await client.depth(symbol=symbol, limit=limit)

            # Извлекаем объемы заявок на покупку (bids) и продажу (asks)
            bids_volume = sum([float(bid[1]) for bid in order_book['bids']])
            asks_volume = sum([float(ask[1]) for ask in order_book['asks']])
            total_liquidity = bids_volume + asks_volume

            # Проверяем, превышает ли общая ликвидность минимальное значение
            return total_liquidity >= min_liquidity
        except Exception as e:
            print(f"Error checking liquidity for {symbol}: {e}")
            return False



def get_quantity(symbol, closed_price, multiplier=multiplier):
    try:
        symbol_info = all_symbols[symbol]
        min_trade_quantity = round(float(symbol_info.notional * multiplier / closed_price), symbol_info.step_size)
        return min_trade_quantity
    except Exception as e:
        print(f"Error getting quantity for {symbol}: {e}")



async def load_symbols():
    global all_symbols, filtered_symbols
    while not shutdown_event.is_set():
        try:
            all_symbols = await client.load_symbols()
            filtered_symbols = await filter_symbols(all_symbols, ignor_list)
            filter_symbols_event.set()
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            print("load_symbols cancelled")
            break
        except Exception as e:
            print(f"Error loading symbols: {e}")
            await asyncio.sleep(3600)

async def create_topics(symbols, timeframe):
    streams_per_connection = 100
    stream_groups = []
    for i in range(0, len(symbols), streams_per_connection):
        stream_group = symbols[i:i + streams_per_connection]
        stream_groups.append(stream_group)
    tasks = []
    for group in stream_groups:
        streams = [f"{symbol.lower()}@kline_{timeframe}" for symbol in group]
        try:
            tasks.append(start_websocket(streams))
        except Exception as e:
            print(f"Error creating task for streams {streams}: {e}")
    try:
        await asyncio.gather(*tasks)
        print("All topics gathered successfully!")
    except Exception as e:
        print(f"Error during gathering tasks: {e}")

async def start_websocket(streams):
    try:
        ws = await client.websocket(streams, on_message=handle_kline)
        klines_ws.append(ws)
        print(f"WebSocket started for streams: {streams}")
    except Exception as e:
        print(f"Error starting WebSocket for streams {streams}: {e}")

async def connect_ws_userdata():
    global my_ws_userdata
    print("Starting WebSockets for userdata")
    try:
        my_ws_userdata = await client.websocket_userdata(on_message=msg_userdata, on_error=ws_error)
    except Exception as e:
        print(f"Error connecting WebSocket for userdata: {e}")

async def ws_error(ws, error):
    print(f"WS ERROR: {error}\n{traceback.format_exc()}")

async def msg_userdata(ws, msg):
    global positions
    try:
        data = msg
        print(f"MSG USERDATA: {data}")
        if "e" in data and data["e"] == "ACCOUNT_UPDATE":
            positions_list = data["a"]["P"]
            for position in positions_list:
                symbol = position["s"]
                if float(position['pa']) != 0:
                    if symbol not in positions:
                        positions.append(symbol)
                else:
                    if symbol in positions:
                        positions.remove(symbol)
        print("Currently we have: ", len(positions), "positions")
    except Exception as e:
        print(f"Error handling userdata message: {e}")

async def handle_kline(ws, msg):
    try:
        if 'data' in msg:
            data = msg['data']
            symbol = data['s']
            kline = data['k']
            closed = kline['x']
            if closed:
                if symbol not in historical_klines:
                    return
                historical_klines[symbol].append(
                    [kline['t'], kline['o'], kline['h'], kline['l'], kline['c'], kline['v'], kline['T']])
                historical_klines[symbol] = historical_klines[symbol][-limit_klines:]
                asyncio.create_task(strategy(symbol, historical_klines[symbol], float(kline['c'])))
                print(f'Klines were updated for {symbol}!')
    except Exception as e:
        print(f"Error handling kline: {e}")


#  check the function 

# def calculate_indicator(indicator_func, candles, results_queue, index):
#     """
#     Функция для вычисления индикатора в отдельном потоке.
#     :param indicator_func: Функция для расчета индикатора
#     :param candles: Данные для расчета
#     :param results_queue: Очередь для результатов
#     :param index: Индекс результата
#     """
#     result = indicator_func(candles)
#     results_queue.put((index, result))

# async def calculate_indicators(symbol, candles):
#     try:
#         results_queue = queue.Queue()

#         # Запуск расчетов индикаторов в отдельных потоках
#         threads = [
#             threading.Thread(target=calculate_indicator, args=(SuperTrend_numpy, candles, results_queue, 0)),
#             threading.Thread(target=calculate_indicator, args=(ATR_numpy, candles, results_queue, 1)),
#             threading.Thread(target=calculate_indicator, args=(ADX_numpy, candles, results_queue, 2))
#         ]

#         for thread in threads:
#             thread.start()

#         for thread in threads:
#             thread.join()

#         results = [None] * 3
#         for _ in range(3):
#             index, result = results_queue.get()
#             results[index] = result

#         supertrend_result, atr_result, adx_result = results

#         # Добавляем отладку для проверки значений
#         print(f"Symbol: {symbol}")
#         print(f"Supertrend result: {supertrend_result[-5:]}")
#         print(f"ATR result: {atr_result[-5:]}")
#         print(f"ADX result: {adx_result[-5:]}")

#         return {
#             'symbol': symbol,
#             'supertrend': supertrend_result,
#             'atr': atr_result,
#             'adx': adx_result
#         }
#     except Exception as e:
#         print(f"Error calculating indicators for {symbol}: {e}")
#         return None





async def strategy(symbol, candles, closed_price, atr_threshold=20, adx_threshold=25):
    try:
        # Получаем результаты индикаторов
        indicators = await calculate_indicators(symbol, candles)

        if indicators is None:
            return

        supertrend = indicators['supertrend']
        atr = indicators['atr']
        adx = indicators['adx']

        # Пример использования индикаторов для принятия решений
        prev_trend = supertrend[-2]
        last_trend = supertrend[-1]

        # Добавляем отладку для проверки условий
        print(f"Previous trend: {prev_trend}, Last trend: {last_trend}")
        print(f"ATR: {atr[-1]}, ATR Threshold: {atr_threshold}")
        print(f"ADX: {adx[-1]}, ADX Threshold: {adx_threshold}")

        if last_trend == 'up' and prev_trend == 'down' and (
                float(atr[-1]) > atr_threshold or float(adx[-1]) > adx_threshold):
            await order_pool.put((symbol, 'BUY', closed_price))
            print('Signal BUY!')

        elif last_trend == 'down' and prev_trend == 'up' and (atr[-1] > atr_threshold or adx[-1] > adx_threshold):
            await order_pool.put((symbol, 'SELL', closed_price))
            print('Signal SELL!')
    except Exception as e:
        print(f"Error in strategy for {symbol}: {e}")




# async def strategy(symbol, candles, closed_price):
#     try:
#         supertrend = SuperTrend_numpy(candles=candles)
#         prev_trend = supertrend[-2]
#         last_trend = supertrend[-1]
#
#         if last_trend == 'up' and prev_trend == 'down':
#             await order_pool.put((symbol, 'BUY', closed_price))
#             print('Signal BUY!')
#
#         elif last_trend == 'down' and prev_trend == 'up':
#             await order_pool.put((symbol, 'SELL', closed_price))
#             print('Signal SELL!')
#     except Exception as e:
#         print(f"Error in strategy for {symbol}: {e}")

async def limited_historical_klines_requests(trading_symbols, limits=50):
    start_time = time.time()
    semaphore = asyncio.Semaphore(limits)

    async def request_with_semaphore(symbol):
        try:
            async with semaphore:
                await set_historical_klines(symbol, timeframe)
        except Exception as e:
            print(f"Error requesting historical klines for {symbol}: {e}")

    tasks = [request_with_semaphore(symbol) for symbol in trading_symbols]
    try:
        await asyncio.gather(*tasks)
        print(f'All klines were requested in {time.time() - start_time} seconds')
    except Exception as e:
        print(f"Error during gathering historical klines: {e}")

# async def set_historical_klines(symbol, interval, limit=limit_klines):
#     try:
#         klines = await client.klines(symbol=symbol, interval=interval, limit=limit)
#         filtered_klines = [kline[:7] for kline in klines]
#         filtered_klines.pop()
#         historical_klines[symbol] = filtered_klines
#         print(f'Klines were received and filtered for {symbol}!')
#     except Exception as e:
#         print(f"Error fetching klines for {symbol}: {e}")

async def set_historical_klines(symbol, interval, limit=limit_klines):
    try:
        # Получаем данные о волатильности и проверяем её
        klines = await client.klines(symbol=symbol, interval=interval, limit=limit)
        # close_prices = [float(kline[4]) for kline in klines]
        # volatility = (max(close_prices) - min(close_prices)) / min(close_prices)
        #
        # # Проверка волатильности
        # if volatility < 0.01:  # Если волатильность меньше 1%
        #     print(f"Low volatility for {symbol}. Skipping...")
        #     return False

        # Если волатильность нормальная, продолжаем обработку
        filtered_klines = [kline[:7] for kline in klines]
        filtered_klines.pop()  # Убираем последнюю запись
        historical_klines[symbol] = filtered_klines
        print(f'Klines were received and filtered for {symbol}!')
        return True
    except Exception as e:
        print(f"Error fetching klines for {symbol}: {e}")
        return False


async def execute_order_pool():
    while not shutdown_event.is_set():
        try:
            symbol, side, closed_price = await order_pool.get()
            can_open_position = len(positions) < max_positions

            # Check for open orders
            # if can_open_position:
            #     # Проверка на открытые ордера
            #     open_orders = await client.get_open_orders(symbol=symbol)
            #     if len(open_orders) > 0:
            #         print(f"Open orders exist for {symbol}, skipping...")
            #         order_pool.task_done()
            #         continue

            # Check liquidity
            if can_open_position:
                if not await check_liquidity(symbol):
                    print(f"Insufficient liquidity for {symbol}. Skipping...")
                    order_pool.task_done()
                    continue

                quantity = await get_quantity(symbol, closed_price)
                try:
                    order = await client.new_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
                    print(f"Order executed: {symbol} {side} {order}")
                except Exception as e:
                    print(f"Error executing order: {e}")
            else:
                print(f"Position limit reached! Cannot open {side} position for {symbol}")
            order_pool.task_done()
        except Exception as e:
            print(f"Error in order pool execution: {e}")


async def main():
    global client
    client = binance.Futures(api_key=API_KEY, secret_key=SECRET_KEY, testnet=True, asynced=True)
    try:
        symbol_task = asyncio.create_task(load_symbols())
        userdata_task = asyncio.create_task(connect_ws_userdata())
        await filter_symbols_event.wait()
        await create_topics(filtered_symbols, timeframe)
        await sleep_to_next_min()
        await limited_historical_klines_requests(filtered_symbols)
        order_pool_task = asyncio.create_task(execute_order_pool())

        print("All symbols loaded", len(all_symbols))

        tasks = [symbol_task, userdata_task, order_pool_task]

        await shutdown_event.wait()
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        for task in tasks:
            task.cancel()
        try:
            await my_ws_userdata.close()
        except Exception as e:
            print(f"Error closing userdata websocket: {e}")
        try:
            await client.close()
        except Exception as e:
            print(f"Error closing client: {e}")
        exit(0)

if __name__ == '__main__':
    asyncio.run(main())
