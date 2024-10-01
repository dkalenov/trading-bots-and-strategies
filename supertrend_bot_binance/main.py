import traceback
import binance
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
open_order_pool = asyncio.Queue()
close_order_pool = asyncio.Queue()
filtered_symbols = []
my_ws_userdata = None
positions = {}
max_positions = 3
multiplier=1.2


adx_threshold = 1  # порог ADX 25
stop_loss_percentage = 2  # Стоп-лосс в процентах от цены входа
take_profit_percentage = 3  # Тейк-профит в процентах от цены входа



async def get_quantity(symbol, closed_price, multiplier=1.2):
    symbol_info = all_symbols[symbol]
    min_trade_quantity = round(float(symbol_info.notional * multiplier / closed_price), symbol_info.step_size)
    return min_trade_quantity

async def load_symbols():
    global all_symbols, filtered_symbols

    while not shutdown_event.is_set():  # Check if the shutdown_event has been triggered
        try:
            all_symbols = await client.load_symbols()
            filtered_symbols = await filter_symbols(all_symbols, ignor_list)
            filter_symbols_event.set()
            await asyncio.sleep(3600)  # Recheck every hour
        except asyncio.CancelledError:
            print("load_symbols cancelled")
            break
        except Exception as e:
            print(f"Error loading symbols: {e}")
            await asyncio.sleep(3600)



async def create_topics(symbols, timeframe):
    streams_per_connection = 100
    stream_groups = []
    # Divide symbols into groups with a max of 100 symbols per group
    for i in range(0, len(symbols), streams_per_connection):
        stream_group = symbols[i:i + streams_per_connection]
        stream_groups.append(stream_group)
    # Create tasks for each group of streams
    tasks = []
    for group in stream_groups:
        streams = [f"{symbol.lower()}@kline_{timeframe}" for symbol in group]
        try:
            tasks.append(start_websocket(streams))
        except Exception as e:
            print(f"Error creating task for streams {streams}: {e}")
    # Run all tasks concurrently
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
    my_ws_userdata = await client.websocket_userdata(on_message=msg_userdata, on_error=ws_error)

async def ws_error(ws, error):
    print(f"WS ERROR: {error}\n{traceback.format_exc()}")


async def msg_userdata(ws, msg):
    """ Обработка сообщений с пользовательскими данными, обновление позиций """
    global positions
    data = msg
    print(f"MSG USERDATA: {data}")

    if "e" in data and data["e"] == "ACCOUNT_UPDATE":
        positions_list = data["a"]["P"]

        for position in positions_list:
            symbol = position["s"]
            quantity = float(position['pa'])
            entry_price = float(position['ep'])

            if quantity != 0:
                # Используем словарь для хранения объема и цены покупки
                positions[symbol] = {'quantity': quantity, 'entry_price': entry_price}
                print(f"Updated positions: {positions}")
            else:
                try:
                    positions.pop(symbol, None)
                except KeyError:
                    print(f"Symbol {symbol} not found in positions")
    print("Actually we have: ", len(positions), "positions")


async def handle_kline(ws, msg):
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
            # print(f'Klines was updated for {symbol}!')



def calculate_indicator(indicator_func, candles, results_queue, index):
    """
    Функция для вычисления индикатора в отдельном потоке.
    :param indicator_func: Функция для расчета индикатора
    :param candles: Данные для расчета
    :param results_queue: Очередь для результатов
    :param index: Индекс результата
    """
    result = indicator_func(candles)
    results_queue.put((index, result))
# #
async def threaded_calculate_indicators(symbol, candles):
    try:
        results_queue = queue.Queue()

        # Запуск расчетов индикаторов в отдельных потоках
        threads = [
            threading.Thread(target=calculate_indicator, args=(SuperTrend_numpy, candles, results_queue, 0)),
            threading.Thread(target=calculate_indicator, args=(ADX_numpy, candles, results_queue, 1))
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        results = [None] * 2
        for _ in range(2):
            index, result = results_queue.get()
            results[index] = result

        supertrend_result, adx_result = results   # adx_result

        # Добавляем отладку для проверки значений
        # print(f"Symbol: {symbol}")
        # print(f"Supertrend result: {supertrend_result[-5:]}")
        # # print(f"ATR result: {adx_result[-5:]}")
        # print(f"ADX result: {adx_result[-5:]}")

        return {
            'symbol': symbol,
            'supertrend': supertrend_result,
            'adx': adx_result
        }
    except Exception as e:
        print(f"Error calculating indicators for {symbol}: {e}")
        return None


async def strategy(symbol, candles, closed_price):
    try:
        indicators = await threaded_calculate_indicators(symbol, candles)
        supertrend = indicators['supertrend']
        adx = indicators['adx']

        prev_trend = supertrend[-2]
        last_trend = supertrend[-1]


        if last_trend == 'up' and prev_trend == 'down': # if last_trend == 'up' and prev_trend == 'down'
            print(f"Previous trend: {prev_trend}, Last trend: {last_trend}")
            print(f"ADX: {adx[-1]}, ADX Threshold: {adx_threshold}")
            if float(adx[-1]) > adx_threshold:
                await open_order_pool.put((symbol, 'BUY', closed_price))

        elif last_trend == 'down' and prev_trend == 'up': # elif last_trend == 'down' and prev_trend == 'up'
            print(f"Previous trend: {prev_trend}, Last trend: {last_trend}")
            print(f"ADX: {adx[-1]}, ADX Threshold: {adx_threshold}")
            if float(adx[-1]) > adx_threshold:
                await open_order_pool.put((symbol, 'SELL', closed_price))

        await check_exit_conditions(symbol, closed_price)

    except Exception as e:
        print(f"Error in strategy for {symbol}: {e}")


async def check_exit_conditions(symbol, closed_price):
    """ Проверяем условия выхода для позиций (стоп-лосс и тейк-профит) и добавляем в очередь закрытия """
    if symbol in positions:
        entry_price = float(positions[symbol]['entry_price'])  # Преобразуем в число для сравнения
        take_profit_level = entry_price * (1 + take_profit_percentage / 100)
        stop_loss_level = entry_price * (1 - stop_loss_percentage / 100)

        print(f"Checking exit conditions for {symbol}: Closed price: {closed_price}, Entry price: {entry_price}, "
              f"Take-profit: {take_profit_level}, Stop-loss: {stop_loss_level}")

        if closed_price >= take_profit_level:
            await close_order_pool.put((symbol, 'SELL'))  # Закрытие через очередь
            print(f"Take-profit hit for {symbol}, added to close order queue.")

        elif closed_price <= stop_loss_level:
            await close_order_pool.put((symbol, 'SELL'))  # Закрытие через очередь
            print(f"Stop-loss hit for {symbol}, added to close order queue.")


async def open_position():
    while not shutdown_event.is_set():
        # Получаем следующий ордер из очереди
        symbol, side, closed_price = await open_order_pool.get()
        can_open_position = True if len(positions) < max_positions else False
        if can_open_position:
            quantity = await get_quantity(symbol, closed_price)
            try:
                # Исполняем ордер
                order = await client.new_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
                print(f"Order executed: {symbol} {side} {order}")

                # # Добавляем позицию в список открытых
                # positions[symbol] = {
                #     'quantity': quantity,
                #     'entry_price': closed_price  # Запоминаем цену входа
                # }

            except Exception as e:
                print(f"Error executing order: {e}")
        else:
            print(f"Position limit reached! Cannot open {side} position for {symbol}")

        # Отмечаем задачу как выполненную
        open_order_pool.task_done()


async def close_position():
    while not shutdown_event.is_set():
        symbol, side = await close_order_pool.get()  # Получаем следующую позицию на закрытие
        try:
            if symbol in positions:
                quantity = abs(positions[symbol]['quantity'])
                order = await client.new_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
                print(f"Closed position: {symbol} {side} {order}")

                # Удаляем позицию из списка после закрытия
                positions.pop(symbol, None)

            else:
                print(f"No position to close for {symbol}")

        except Exception as e:
            print(f"Error closing position for {symbol}: {e}")

        close_order_pool.task_done()



async def limited_historycal_klines_requests(trading_symbols, limits=50):
    start_time = time.time()
    semaphore = asyncio.Semaphore(limits)

    async def request_with_semaphore(symbol):
        async with semaphore:
            await set_historycal_klines(symbol, timeframe)

    tasks = [request_with_semaphore(symbol) for symbol in trading_symbols]
    await asyncio.gather(*tasks)

    # print(f'All klines were requested in {time.time() - start_time} seconds')


async def set_historycal_klines(symbol, interval, limit=limit_klines):
    try:
        klines = await client.klines(symbol=symbol, interval=interval, limit=limit)
        # Limit each kline data to the first 7 elements
        filtered_klines = [kline[:7] for kline in klines]
        filtered_klines.pop()
        # Store the filtered klines in the historycal_klines dictionary
        historical_klines[symbol] = filtered_klines
        # print(f'Klines were received and filtered for {symbol}!')
    except Exception as e:
        print(f"Error fetching klines for {symbol}: {e}")


# async def execute_order_pool():
#     while not shutdown_event.is_set():
#         # Get the next order from the queue
#         symbol, side, closed_price = await order_pool.get()
#         can_open_position = True if len(positions) < max_positions else False
#         if can_open_position:
#             quantity = await get_quantity(symbol, closed_price)
#             try:
#                 order = await client.new_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
#                 print(f"Order executed: {symbol} {side} {order}")
#             except Exception as e:
#                 print(f"Error executing order: {e}")
#             # Send a notification (if needed)
#         else:
#             print(f"Position limit reached! Now there're {len(positions)} positions. Cannot open {side} position for {symbol}")
#         # Mark the order as done
#         order_pool.task_done()

async def main():
    global client
    client = binance.Futures(api_key=API_KEY, secret_key=SECRET_KEY, testnet=True, asynced=True)

    symbol_task = asyncio.create_task(load_symbols())
    userdata_task = asyncio.create_task(connect_ws_userdata())
    await filter_symbols_event.wait()
    await create_topics(filtered_symbols, timeframe)
    await sleep_to_next_interval(timeframe)
    await limited_historycal_klines_requests(filtered_symbols)

    # Запускаем задачу для открытия позиций
    order_pool_task = asyncio.create_task(open_position())

    # Запускаем задачу для закрытия позиций
    close_order_pool_task = asyncio.create_task(close_position())

    tasks = [symbol_task, userdata_task, order_pool_task, close_order_pool_task]

    try:
        await shutdown_event.wait()
    finally:
        for task in tasks:
            task.cancel()
        await my_ws_userdata.close()
        await client.close()
        exit(0)


if __name__ == '__main__':
    asyncio.run(main())
