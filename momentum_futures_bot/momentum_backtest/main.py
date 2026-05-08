import configparser
import logging
import binance
import db
from datetime import datetime, timezone, timedelta, time
from concurrent.futures import ThreadPoolExecutor
import utils
import traceback
import get_data
from sqlalchemy import select, update
from datetime import datetime
from collections import defaultdict
import asyncio
from asyncio import Future
import tg
import time






# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
session = None
executor = ThreadPoolExecutor(max_workers=30)
all_symbols: dict[str, binance.SymbolFutures] = {}
all_prices: dict[str, binance.SymbolFutures] = {}


IMPORTANT_SYMBOLS = ['BTCUSDT', 'ETHUSDT']
VALID_SIGNALS = ['STRONG_BUY', 'STRONG_SELL']


timeframes = ["4h"]
btc_signal = None

# список открытых позиций
positions = {}

websockets_list: list[binance.futures.WebsocketAsync] = []
debug = None
symbol_conf_cache: dict[str, db.SymbolsSettings] = {}
in_progress = {}
userdata_ws = None
symbol_locks = defaultdict(asyncio.Lock)

updated_take_stop: dict[str, tuple[float, float]] = {}
updated_event = asyncio.Event()
message_queue = asyncio.Queue()
error_queue = asyncio.Queue()

symbol_locks = defaultdict(asyncio.Lock)



# --- ⏱ Антиспам параметры ---
error_send_cache = defaultdict(lambda: 0)  # key -> время последней отправки
ERROR_SEND_INTERVAL = 300  # 5 минут




async def main():
    global session, conf, client, all_symbols, all_prices, debug, symbol_conf_cache, positions

    config.read('config.ini')

    # подключение к БД
    session = await db.connect(
        config['DB']['host'],
        int(config['DB']['port']),
        config['DB']['user'],
        config['DB']['password'],
        config['DB']['db']
    )

    # загрузка конфига
    conf = await db.load_config()

    # включаем режим отладки из .ini
    debug = config.getboolean('BOT', 'debug')

    # проверка наличия API ключей
    if not conf.api_key or not conf.api_secret:
        print("API ключи не заданы. Запущен только Telegram-бот.")
        await tg.run(session, None, connect_ws, disconnect_ws, subscribe_ws, unsubscribe_ws)
        return

    # инициализация клиента Binance
    client = binance.Futures(
        conf.api_key,
        conf.api_secret,
        asynced=True,
        testnet=config.getboolean('BOT', 'testnet')
    )

    # загрузка конфигов символов
    symbol_confs = await db.get_all_symbols_conf()
    symbol_conf_cache = {s.symbol: s for s in symbol_confs}

    # загрузка всех позиций
    await get_data.sync_positions_with_exchange(client, positions)
    open_symbols = [symbol for symbol, status in positions.items() if status]
    print(f"Открытых позиций: {len(open_symbols)}")

    # загрузка символов и цен с Binance
    all_symbols = await get_data.load_binance_symbols(client)
    all_prices = await get_data.get_all_prices(client)

    # запуск параллельных задач
    symbol_update_lock = asyncio.Lock()
    await asyncio.gather(
        *(timed_collector(tf, symbol_update_lock) for tf in timeframes),
        db.periodic_symbol_update(client, executor, symbol_update_lock, hour=17, minute=37),
        tg.run(session, client, connect_ws, disconnect_ws, subscribe_ws, unsubscribe_ws),
        message_sender(),
        error_sender(),
        connect_ws()
    )




async def timed_collector(timeframe: str, lock: asyncio.Lock):
    global symbol_conf_cache, all_symbols, all_prices, conf

    while True:
        await utils.wait_for_next_candle(timeframe)
        async with lock:
            try:
                # ✅ Обновляем кэш 1 раз на таймфрейм
                symbol_confs = await db.get_all_symbols_conf()
                symbol_conf_cache = {s.symbol: s for s in symbol_confs}

                all_symbols = await get_data.load_binance_symbols(client)
                all_prices = await get_data.get_all_prices(client)

                if timeframe == timeframes[0]:
                    conf = await db.load_config()

                    await get_data.sync_positions_with_exchange(client, positions)

                    await process_main_timeframe_signals()
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(5)
                    await collect_signals(timeframe)

            except Exception as e:
                logging.error(f"[{timeframe}] Ошибка в сборщике сигналов: {e}")




async def collect_signals(timeframe=timeframes[0]):
    available_symbols = list(symbol_conf_cache.keys())
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in available_symbols if s not in IMPORTANT_SYMBOLS]

    loop = asyncio.get_running_loop()
    tasks = [process_symbol(symbol, timeframe, loop) for symbol in symbols_ordered]
    results = await asyncio.gather(*tasks)

    signals = [r for r in results if r is not None]

    if signals:
        await db.save_signals_batch_to_db(signals)




async def process_symbol(symbol, interval, loop):
    data = await loop.run_in_executor(executor, get_data.get_tradingview_data, symbol, interval)
    if not data:
        return None

    entry_price = all_prices.get(symbol)
    if entry_price is None:
        logging.warning(f"Цена не найдена для {symbol}")
        return None

    return (symbol, interval, data['RECOMMENDATION'], entry_price)




async def process_main_timeframe_signals():
    global btc_signal
    logging.info(f"Обработка сигналов {timeframes[0]} и открытие сделок...")

    interval = timeframes[0]

    loop = asyncio.get_running_loop()
    btc_data = await loop.run_in_executor(executor, get_data.get_tradingview_data, 'BTCUSDT', interval)
    if not btc_data:
        logging.error("Не удалось получить сигнал BTCUSDT — пропускаем обработку.")
        return
    btc_signal = btc_data['RECOMMENDATION']
    logging.info(f"Сигнал BTCUSDT {timeframes[0]}: {btc_signal}")

    available_symbols = list(symbol_conf_cache.keys())
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in available_symbols if s not in IMPORTANT_SYMBOLS]


    tasks = [process_trade_signal(symbol, interval) for symbol in symbols_ordered]
    results = await asyncio.gather(*tasks)

    print(f"Результаты сигналов: {results}")

    signals_to_save = [r for r in results if r is not None]
    print('SIGNAL TO SAVE', datetime.now(timezone.utc), signals_to_save)

    if signals_to_save:
        logging.info(f"Сохраняем {len(signals_to_save)} сигналов в БД")
        await db.save_signals_batch_to_db(signals_to_save)
    else:
        logging.warning("Нет сигналов для сохранения — список пуст.")




async def process_trade_signal(symbol, interval):
    global debug, conf
    signal_to_return = None
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(executor, get_data.get_tradingview_data, symbol, interval)
        if not data:
            print('NO DATA')
            return None

        entry_price = all_prices.get(symbol)
        if entry_price is None:
            logging.warning(f"Цена не найдена для {symbol}, сохраняем с NaN")
            entry_price = float('nan')

        recommendation = data['RECOMMENDATION']
        signal_to_return = (symbol, interval, recommendation, entry_price)
        # print(f'SIGNAL TO RETURN {datetime.now(timezone.utc)} {signal_to_return}')

        if not conf.trade_mode:
            return signal_to_return

        if not conf.api_key or not conf.api_secret:
            await db.config_update(trade_mode='0')
            return signal_to_return

        symbol_conf = symbol_conf_cache.get(symbol)
        if not symbol_conf or not symbol_conf.status:
            return signal_to_return

        if debug:
            open_long = recommendation in ['STRONG_BUY', 'BUY', 'NEUTRAL']
            # open_long = recommendation in ['NEUTRAL']
            # open_long = recommendation in ['STRONG_BUY', 'BUY', 'NEUTRAL', 'STRONG_SELL', 'SELL']
            open_short = recommendation in ['STRONG_SELL', 'SELL']
            # open_short = recommendation in ['STRONG_BUY', 'BUY', 'NEUTRAL', 'STRONG_SELL', 'SELL']
        else:
            open_long = recommendation == 'STRONG_BUY' and btc_signal in ['STRONG_BUY', 'BUY', 'NEUTRAL']
            open_short = recommendation == 'STRONG_SELL' and btc_signal in ['STRONG_SELL', 'SELL', 'NEUTRAL']

        signal = None
        if open_long:
            signal = "BUY"
        elif open_short:
            signal = "SELL"

        # print('POSITIONS PROCESS TRADE SIGNAL RUN', datetime.now(timezone.utc), positions)
        # if signal and not positions.get(symbol, False):
        #     logging.info(f"Открытие {signal} по {symbol} @ {entry_price} | BTC = {btc_signal}")
        #     await new_trade(symbol, interval, signal)

        if signal:
            async with symbol_locks[symbol]:
                if not positions.get(symbol, False):
                    logging.info(f"Открытие {signal} по {symbol} @ {entry_price} | BTC = {btc_signal}")
                    await new_trade(symbol, interval, signal)

        return signal_to_return

    except Exception as e:
        logging.exception(f"Ошибка при обработке сигнала {symbol}: {e}")
        return signal_to_return




async def message_sender():
    while True:
        chat_id, text, future = await message_queue.get()
        try:
            while True:
                try:
                    msg = await tg.bot.send_message(chat_id, text, parse_mode='HTML')
                    future.set_result(msg.message_id)
                    await asyncio.sleep(1.2)
                    break
                except Exception as e:
                    if "retry after" in str(e).lower():
                        import re
                        match = re.search(r"retry after (\d+)", str(e), re.IGNORECASE)
                        if match:
                            delay = int(match.group(1)) + 1
                            logging.warning(f"Flood control (notify): ждем {delay} сек.")
                            await asyncio.sleep(delay)
                            continue
                    raise e
        except Exception as e:
            logging.error(f"Ошибка отправки сообщения: {e}")
            future.set_exception(e)
        message_queue.task_done()




# Отправка критических ошибок с защитой от спама
async def notify_critical_error(message: str, key: str = None, force_send=False):
    now = time.time()
    key = key or message  # Если ключ не указан — используем сам текст

    if force_send or (now - error_send_cache[key] >= ERROR_SEND_INTERVAL):
        error_send_cache[key] = now
        text = f"🚨 <b>Критическая ошибка</b>\n\n<code>{message}</code>"

        future = Future()
        await error_queue.put((config['TG']['error_channel'], text, future))
        try:
            await future
            logging.warning(f"[Telegram Notify] Сообщение отправлено: {key}")
        except Exception as e:
            logging.error(f"Ошибка при отправке критического уведомления в Telegram: {e}")
    else:
        logging.info(f"[Telegram Notify] Подавлено повторное сообщение: {key}")






async def error_sender():
    while True:
        chat_id, text, future = await error_queue.get()
        try:
            while True:
                try:
                    msg = await tg.bot.send_message(chat_id, text, parse_mode='HTML')
                    future.set_result(msg.message_id)
                    await asyncio.sleep(1.5)  # защита от флуда
                    break
                except Exception as e:
                    if "retry after" in str(e).lower():
                        # Извлекаем количество секунд из сообщения
                        import re
                        match = re.search(r"retry after (\d+)", str(e), re.IGNORECASE)
                        if match:
                            delay = int(match.group(1)) + 1
                            logging.warning(f"Telegram flood control: ждем {delay} сек.")
                            await asyncio.sleep(delay)
                            continue
                    raise e  # другие ошибки не повторяем
        except Exception as e:
            logging.error(f"❌ Ошибка при отправке ошибки: {e}")
            future.set_exception(e)
        error_queue.task_done()










async def new_trade(symbol, interval, signal):
    global positions
    loop = asyncio.get_running_loop()


    # --- 🚫 Блокировка при уже активной позиции ---
    if positions.get(symbol):
        logging.warning(f"{symbol}: позиция уже открыта (по флагу positions), повторный вход запрещён.")
        return

    try:
        # --- 🔎 Проверка позиции на Binance ---
        binance_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binance_positions if p["symbol"] == symbol), None)

        if position_info and abs(float(position_info["positionAmt"])) > 0:
            logging.warning(f"{symbol}: позиция уже открыта на Binance, повторный вход запрещён.")
            positions[symbol] = True  # Синхронизируем флаг
            return

        # --- 🔎 Проверка в БД ---
        # existing_trade = await db.get_open_trade(symbol)
        # if existing_trade and existing_trade.position_open:
        #     logging.warning(f"{symbol}: открытая сделка уже есть в БД (id={existing_trade.id}), вход запрещён.")
        #     positions[symbol] = True  # Синхронизируем флаг
        #     return

    except Exception as e:
        logging.error(f"{symbol}: ошибка при проверке существующей позиции: {e}")
        return

    try:
        try:
            klines = await client.klines(symbol, interval=interval, limit=150)
            # print(f"{symbol} klines{klines}")
        except Exception as e:
            logging.error(f"Ошибка при получении свечей: {e}")
            return

        # получаем информацию о символе
        if not (symbol_info := all_symbols.get(symbol)):
            print('NO SYMBOL INFO')
            return

        #получаем настройки для символа
        try:
            symbol_conf = symbol_conf_cache.get(symbol)
            # print(f"{symbol}: {symbol_conf}")
        except Exception as e:
            logging.error(f"Ошибка при получении конфигурации символа {symbol}: {e}")
            return

        atr_length = symbol_conf.atr_length
        # print(f"{symbol} atr: {atr_length}")
        try:
            df = await loop.run_in_executor(executor, utils.calculate_atr, klines, atr_length)
        except Exception as e:
            logging.error(f"Ошибка при расчёте atr: {e}")
            return

        last_row = df.iloc[-1]
        atr = float(last_row["ATR"])
        # print(f'{symbol}, atr:{atr}')

        #получаем последнюю цену
        last_price = all_prices.get(symbol)
        # print(f"LAST PRICE {symbol}: {last_price}")

        # расчитываем количество
        quantity = utils.round_down(symbol_conf.order_size / last_price, symbol_info.step_size)
        # print(f"{symbol} QUANTITY: {quantity}")

        # проверяем чтобы объем был больше минимального
        min_notional = symbol_info.notional * 1.1
        if quantity * last_price < min_notional:
            # если объем меньше минимального, то берем минимальный объем
            quantity = utils.round_up(min_notional / last_price, symbol_info.step_size)
        # создаем ордер на вход в позицию
        # print(f"Открываем позицию по {symbol}")
        entry_order = await client.new_order(symbol=symbol, side='BUY' if signal == "BUY" else 'SELL', type='MARKET',
                                             quantity=quantity, newOrderRespType='RESULT')



        # order_info = await db.get_active_entry_order_info(symbol, client)
        #
        # if order_info:
        #     print("ℹ️ Инфо по входному ордеру:", order_info)





    except Exception as e:

        msg = f"🚫 Ошибка при открытии {'ЛОНГОВОЙ' if signal == 'BUY' else 'ШОРТОВОЙ'} позиции по {symbol}\n{e}"
        print(msg)
        logging.exception(msg)

        # Убираем флаг, чтобы попытка могла повториться позже
        positions.pop(symbol, None)

        # Уникальный ключ для антиспама
        key = f"open_trade_error:{symbol}:{signal}"
        await notify_critical_error(msg, key=key)
        return


    try:
        # получаем цену входа
        entry_price = float(entry_order['avgPrice'])
        # получаем количество из ордера
        quantity = float(entry_order['executedQty'])
        # расчитываем цены стопа и тейка
        if signal == "BUY":
            take_price1 = entry_price + atr * symbol_conf.take1
            take_price2 = entry_price + atr * symbol_conf.take2
            stop_price = entry_price - atr * symbol_conf.stop
        else:
            take_price1 = entry_price - atr * symbol_conf.take1
            take_price2 = entry_price - atr * symbol_conf.take2
            stop_price = entry_price + atr * symbol_conf.stop

        # округляем
        take_price1 = round(take_price1, symbol_info.tick_size)
        take_price2 = round(take_price2, symbol_info.tick_size)
        stop_price = round(stop_price, symbol_info.tick_size)
        # создаем ордеры на стоп и тейк
        stop_order = await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='STOP_MARKET',
                                            quantity=quantity, stopPrice=stop_price, timeInForce='GTE_GTC', reduceOnly=True)
        take_order = await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='LIMIT',
                                            quantity=quantity, price=take_price2, timeInForce='GTC', reduceOnly=True)
        print(f"Открыл {signal} позицию по {symbol} по цене {entry_price} с тейком {take_price2} и стопом {stop_price}")
        # создаем сессию для работы с базой данных

        positions[symbol] = True


        async with session() as s:
            # записывает сделку в БД
            trade = db.Trades(symbol=symbol, order_size=float(entry_order['cumQuote']), side = 1 if signal == "BUY" else 0,
                              status='NEW', open_time=entry_order['updateTime'], interval=interval, leverage=symbol_conf.leverage,
                              atr_length=atr_length, atr=atr, entry_price=entry_price, quantity=quantity, take1_price=take_price1,
                              take2_price=take_price2, stop_price=stop_price, take1_triggered=False, position_open=True)



            s.add(trade)
            # отправляем данные в БД
            await s.commit()
            # записываем ордера в БД
            for order in (entry_order, stop_order, take_order):
                s.add(db.Orders(order_id=order['orderId'], trade_id=trade.id, symbol=symbol, time=order['updateTime'],
                                side=order['side'] == 'BUY', type=order['type'], status=order['status'],
                                reduce=order['reduceOnly'], price=float(order['avgPrice']),
                                quantity=float(order['executedQty'])))
            # отправляем данные в БД
            await s.commit()
            try:
                # формируем текст поста в канал
                text = (f"Открыл в <b>{'ЛОНГ' if signal == 'BUY' else 'ШОРТ'}</b> {quantity} <b>{symbol}</b>\n"
                        f"Цена входа: <b>{entry_price}</b>\n"
                        f"Тейк 1: <b>{take_price1}</b>\n"
                        f"Тейк 2: <b>{take_price2}</b>\n"
                        f"Стоп: <b>{stop_price}</b>\n"
                        )
                # отправляем сообщение в канал
                # msg = await tg.bot.send_message(config['TG']['channel'], text, parse_mode='HTML')
                # # записываем идентификатор сообщения в БД
                # trade.msg_id = msg.message_id
                # await s.commit()

                # создаём объект future
                future = Future()
                # добавляем в очередь: (чат, текст, future)
                await message_queue.put((config['TG']['channel'], text, future))
                # ждём результат (message_id или ошибка)
                msg_id = await future
                # записываем его в БД
                trade.msg_id = msg_id
                await s.commit()

            except Exception as e:
                print(f"Ошибка при отправки сообщения для {symbol}, {e}")


    except Exception as e:
        print(f"Ошибка при выставлении стопа и тейка по {symbol}\n{e}")

        try:
            await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='MARKET',
                                   quantity=quantity, reduceOnly=True)

        except Exception as e:
            close_msg = f"‼️ Ошибка при принудительном закрытии сделки по {symbol} ({signal}\n{e})"
            logging.exception(close_msg)
            await notify_critical_error(close_msg, key=f"force_close_error:{symbol}:{signal}")

        # Основное сообщение об ошибке тейка/стопа
        msg = f"🚨 Ошибка при выставлении стопа/тейка по {symbol} ({signal}\n{e})"
        logging.exception(msg)
        await notify_critical_error(msg, key=f"tp_sl_error:{symbol}:{signal}")
        try:
            positions[symbol] = False
        except Exception as e:
            logging.error(f"Ошибка при смене флага на False при positions[symbol]: {e}")




# подключение к вебсокетам
async def connect_ws():
    global websockets_list, userdata_ws
    streams = []

    print("🔌 Подключаемся к WebSocket'ам...")
    conf = await db.load_config()
    if not conf.trade_mode or not conf.api_key or not conf.api_secret:
        await db.config_update(trade_mode='0')
        return


    available_symbols = list(symbol_conf_cache.keys())
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in available_symbols if s not in IMPORTANT_SYMBOLS]

    for symbol in symbols_ordered:
        conf = symbol_conf_cache.get(symbol)
        # print(f"WS {symbol}")
        if conf and conf.status:
            # print('STREAMS APPENDED')
            streams.append(f"{symbol.lower()}@kline_{conf.interval}")

    chunk_size = 100
    streams_list = [streams[i:i + chunk_size] for i in range(0, len(streams), chunk_size)]

    for stream_list in streams_list:
        # запускаем вебсокеты
        websockets_list.append(await client.websocket(stream_list, on_message=ws_msg, on_error=ws_error))
    # подключение к вебсокету userdata
    userdata_ws = await client.websocket_userdata(on_message=ws_user_msg, on_error=ws_error)





# отключение от вебсокетов
async def disconnect_ws():
    global websockets_list, userdata_ws, indicators

    print("🔌 Отключаемся от WebSocket'ов...")
    for ws in websockets_list:
        try:
            await ws.close()
        except:
            pass

    indicators = {}

    try:
        await userdata_ws.close()
    except:
        pass

# подписка на стрим свечей по символу
async def subscribe_ws(symbol, interval):
    global websockets_list
    stream = f"{symbol.lower()}@kline_{interval}"
    for ws in websockets_list:
        if len(ws.stream) < ws.streams_limit:
            await ws.subscribe([stream])
            return

    ws = await client.websocket([stream], on_message=ws_msg, on_error=ws_error)
    websockets_list.append(ws)

# отписка от стрима по символу
async def unsubscribe_ws(symbol):
    global websockets_list
    for ws in websockets_list:
        for stream in ws.stream:
            if stream.split('@')[0] == symbol.lower():
                await ws.unsubscribe(stream)

# обработка ошибок вебсокета
async def ws_error(ws, error):
    print(f"❌ WS ERROR: {error}")
    print(traceback.format_exc())




async def ws_msg(ws, msg):
    if 'data' not in msg:
        return

    kline = msg['data']['k']
    symbol = kline['s']
    current_price = float(kline['c'])

    try:
        trade = await db.get_open_trade(symbol)
        if not trade:
            return

        direction = "BUY" if trade.side else "SELL"

        take1_hit = current_price >= trade.take1_price if direction == "BUY" else current_price <= trade.take1_price
        take2_hit = current_price >= trade.take2_price if direction == "BUY" else current_price <= trade.take2_price
        stop_hit = current_price <= trade.stop_price if direction == "BUY" else current_price >= trade.stop_price

        close_breakeven_hit = False
        if trade.breakeven_stop_price:
            close_breakeven_hit = (
                current_price <= trade.breakeven_stop_price
                if direction == "BUY"
                else current_price >= trade.breakeven_stop_price
            )

        # --- Частичное закрытие (Take1) ---
        if not trade.take1_triggered and take1_hit:
            logging.info(f"{symbol}: достигнут Take1 {trade.take1_price}. Частичное закрытие и перенос стопа.")

            async with session() as s:
                await s.execute(
                    update(db.Trades)
                    .where(db.Trades.id == trade.id)
                    .values(take1_triggered=True)
                )
                await s.commit()

            asyncio.create_task(partial_close_and_move_stop(trade))
            return

        # --- Закрытие по breakeven, stop или take2 ---
        # if close_breakeven_hit or stop_hit or take2_hit:
        #     close_type = (
        #         "БУ" if close_breakeven_hit else
        #         "СТОП" if stop_hit else
        #         "ТЕЙК2"
        #     )
        #     logging.info(f"{symbol}: сработал {close_type}. Отправка STOP_MARKET на закрытие позиции.")


    except Exception as e:
        logging.error(f"{symbol}: ошибка в ws_msg: {e}")




async def ws_user_msg(ws, msg):
    global positions

    event_type = msg.get('e')
    if event_type == 'ACCOUNT_UPDATE':
        for pos in msg['a']['P']:
            symbol = pos['s']
            position_amt = float(pos['pa'])
            positions[symbol] = position_amt != 0.0

    elif event_type == 'ORDER_TRADE_UPDATE':
        o = msg['o']
        symbol = o['s']

        if not o.get('R') or o['X'] not in ('FILLED', 'PARTIALLY_FILLED', 'CANCELLED', 'EXPIRED'):
            return

        # Извлекаем нужные значения прямо из WebSocket
        realized_profit = float(o.get('rp') or 0.0)
        price = float(o.get('ap') or 0.0)
        quantity = float(o.get('z') or 0.0)
        side = o['S'] == 'BUY'
        reduce = o['R']
        status = o['X']
        order_id = o['i']

        # Получаем ордер и трейд
        order, trade = await db.get_order_trade(order_id)

        if not order:
            trade = await db.get_open_trade(symbol)
            if trade:
                async with session() as s:
                    order = db.Orders(
                        order_id=order_id,
                        trade_id=trade.id,
                        symbol=symbol,
                        time=o['T'],
                        side=side,
                        type=o['o'],
                        status='NEW',
                        reduce=reduce,
                        price=price,
                        quantity=quantity,
                        realized_profit=realized_profit
                    )
                    s.add(order)
                    await s.commit()

        if order and trade:
            order.status = status
            order.time = o['T']
            order.price = price
            order.quantity = quantity
            order.realized_profit = realized_profit

            await db.update_order_trade(order, trade)
            await db.update_trade_result(trade.id)

            position_open = positions.get(symbol, False)

            # --- Частичное закрытие (Take1) ---
            if (
                trade.take1_triggered and
                not trade.partial_exit_done and
                order.type == 'MARKET' and
                reduce
            ):
                trade.partial_exit_done = True

                await updated_event.wait()  # дождаться, пока словарь обновится
                stop_take = updated_take_stop.get(symbol)
                if not stop_take:
                    logging.error(f"{symbol}: не удалось получить stop2/take2 из updated_take_stop")
                    return

                stop2, take2 = stop_take
                updated_event.clear()  # готов к следующему обновлению

                text = (
                    f"<b>{symbol}</b>: частично закрыта позиция <b>{'ЛОНГ' if trade.side else 'ШОРТ'}</b>\n"
                    f"Объём: <b>{quantity}</b>\n"
                    f"Цена закрытия: <b>{price}</b>\n"
                    f"{'Прибыль' if realized_profit >= 0 else 'Убыток'}: <b>{round(realized_profit, 4)} USDT</b>\n"
                    f"🛡️ Стоп перенесён в БУ: <b>{stop2}</b>\n"
                    f"🎯 Тейк2 установлен: <b>{take2}</b>"
                )

                try:
                    if trade.msg_id:
                        await tg.bot.send_message(
                            config['TG']['channel'],
                            text,
                            reply_to_message_id=trade.msg_id,
                            parse_mode='HTML'
                        )
                except Exception as e:
                    logging.error(f"{symbol}: ошибка при отправке уведомления (Take1): {e}")

                async with session() as s:
                    await s.execute(
                        update(db.Trades).where(db.Trades.id == trade.id).values(partial_exit_done=True)
                    )
                    await s.commit()

            # --- Полное закрытие ---
            # if not position_open and not trade.status.startswith('CLOSED'):
            #     trade.close_time = int(datetime.now(timezone.utc).timestamp() * 1000)
            #     trade.position_open = False
            #
            #     ot = o.get('ot')
            #     if ot == 'STOP_MARKET':
            #         trade.status = 'CLOSED_BREAKEVEN' if trade.take1_triggered and trade.breakeven_stop_price else 'CLOSED_STOP'
            #     elif ot == 'LIMIT':
            #         trade.status = 'CLOSED_TAKE'
            #     else:
            #         trade.status = 'CLOSED_MARKET'
            #
            #     await db.update_order_trade(order, trade)
            #     await db.update_trade_result(trade.id)
            #
            #     status_text = {
            #         'CLOSED_BREAKEVEN': '🔄 стоп в БУ (после Take1)',
            #         'CLOSED_STOP': '⛔️ по СТОПУ',
            #         'CLOSED_TAKE': '🎯 по Take2',
            #         'CLOSED_MARKET': '📉 по рынку'
            #     }.get(trade.status, '📉 закрыта')

            # --- Полное закрытие ---
            if not position_open and not trade.status.startswith('CLOSED'):
                trade.close_time = int(datetime.now(timezone.utc).timestamp() * 1000)
                trade.position_open = False

                ot = o.get('ot')
                if ot == 'STOP_MARKET':
                    trade.status = 'CLOSED_BREAKEVEN' if trade.take1_triggered and trade.breakeven_stop_price else 'CLOSED_STOP'
                elif ot == 'LIMIT':
                    trade.status = 'CLOSED_TAKE'
                else:
                    trade.status = 'CLOSED_MARKET'

                # отмена оставшегося стопа
                active_stop = await db.get_last_active_stop_order(trade.id)
                if active_stop:
                    try:
                        await client.cancel_order(symbol=symbol, orderId=active_stop.order_id)
                        logging.info(
                            f"{symbol}: оставшийся стоп-ордер {active_stop.order_id} отменён после полного закрытия")
                        async with session() as s:
                            active_stop.status = 'CANCELED'
                            s.add(active_stop)
                            await s.commit()
                    except Exception as e:
                        logging.exception(f"{symbol}: ошибка при отмене оставшегося стопа после закрытия позиции\n{e}")
                        msg = f"{symbol}:❌ ошибка при отмене оставшегося стопа после закрытия позиции\n{e}"
                        # await notify_critical_error(msg, key=f"{symbol}_new_stop_error")

                await db.update_order_trade(order, trade)
                await db.update_trade_result(trade.id)

                # Далее идёт уведомление в Telegram
                status_text = {
                    'CLOSED_BREAKEVEN': '🔄 стоп в БУ (после Take1)',
                    'CLOSED_STOP': '⛔️ по СТОПУ',
                    'CLOSED_TAKE': '🎯 по Take2',
                    'CLOSED_MARKET': '📉 по рынку'
                }.get(trade.status, '📉 закрыта')

                text = (
                    f"<b>{symbol}</b> сделка в <b>{'ЛОНГ' if trade.side else 'ШОРТ'}</b> "
                    f"закрыта {status_text}\n"
                    f"Цена закрытия: <b>{price}</b>\n"
                    f"{'Прибыль' if realized_profit > 0 else 'Убыток'}: <b>{round(realized_profit, 4)} USDT</b>"
                )

                try:
                    if trade.msg_id:
                        await tg.bot.send_message(
                            config['TG']['channel'],
                            text,
                            reply_to_message_id=trade.msg_id,
                            parse_mode='HTML'
                        )
                except Exception as e:
                    logging.error(f"{symbol}: ошибка при отправке уведомления (закрытие): {e}")

                positions.pop(symbol, None)
                print(f"Сделка по {symbol} закрыта.")





async def partial_close_and_move_stop(trade):
    global positions, updated_take_stop

    symbol = trade.symbol
    portion = symbol_conf_cache.get(symbol).portion
    step_size = all_symbols[symbol].step_size
    min_qty = all_symbols[symbol].min_qty
    entry_price = trade.entry_price
    tick_size = all_symbols[symbol].tick_size


    direction = "BUY" if trade.side else "SELL"
    close_side = "SELL" if direction == "BUY" else "BUY"


    if trade.partial_exit_done:
        logging.info(f"{symbol}: частичное закрытие уже выполнено ранее, пропуск.")
        return

    try:
        binance_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binance_positions if p["symbol"] == symbol), None)

        if not position_info:
            logging.warning(f"{symbol}: позиция не найдена.")
            return

        position_amt = abs(float(position_info["positionAmt"]))
        if position_amt == 0:
            logging.warning(f"{symbol}: позиция уже закрыта.")
            return

        qty = utils.round_down(trade.quantity * portion, step_size)

        if qty < min_qty:
            logging.warning(f"{symbol}: объём {qty} меньше минимального лота {min_qty}, пропускаем частичное закрытие.")

        else:
            portion_close_order = await client.new_order(
                symbol=symbol,
                side=close_side,
                type='MARKET',
                quantity=qty,
                reduceOnly=True
            )
            logging.info(f"{symbol}: частично закрыто {portion * 100:.1f}% позиции ({qty})")



    except Exception as e:
        logging.exception(f"{symbol}: ошибка при MARKET-закрытии части позиции: {e}")
        msg = f"{symbol}: ошибка при MARKET-закрытии части позиции\n{e}"
        await notify_critical_error(msg, key=f"{symbol}_partial_market_close")
        return


    # --- Проверка оставшейся позиции ---
    try:
        binance_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binance_positions if p["symbol"] == symbol), None)
        remaining_amt = abs(float(position_info["positionAmt"]))

        if remaining_amt == 0:
            logging.info(f"{symbol}: позиция полностью закрыта после частичного выхода.")
            text = (f"{symbol}: позиция полностью закрыта после частичного выхода.")
            try:
                if trade.msg_id:
                    await tg.bot.send_message(
                        config['TG']['channel'],
                        text,
                        reply_to_message_id=trade.msg_id,
                        parse_mode='HTML'
                    )
            except Exception as e:
                logging.info(f"{symbol}: позиция полностью закрыта после частичного выхода. Сообщение не отправлено в ТГ")

            positions[symbol] = False
            return

        remaining_qty = utils.round_down(remaining_amt, step_size)
        new_stop = round(entry_price * (0.999 if direction == "BUY" else 1.001), tick_size)
        take2_price = trade.take2_price

        old_stop_order = await db.get_last_active_stop_order(trade.id)
        try:
            # --- Новый стоп ---
            new_stop_order = await client.new_order(
                symbol=symbol,
                side=close_side,
                type='STOP_MARKET',
                stopPrice=new_stop,
                quantity=remaining_qty,
                reduceOnly=True
            )
            logging.info(f"{symbol}: новый стоп установлен @ {new_stop}, объём {remaining_qty}")
        except Exception as e:

            logging.exception(f"{symbol}: ❌ ошибка при установке нового стопа @ {new_stop}, объём {remaining_qty}\n{e}")
            msg = f"{symbol}: ❌ ошибка при установке нового стопа @ {new_stop}, объём {remaining_qty}\n{e}"

            await notify_critical_error(msg, key=f"{symbol}_new_stop_error")

        if qty < min_qty:
            logging.warning(f"{symbol}: объём {qty} меньше минимального лота. Оставляем старый стоп")
        else:
        # --- Новый тейк2 ---
            try:
                new_take2_order = await client.new_order(
                    symbol=symbol,
                    side=close_side,
                    type='LIMIT',
                    price=take2_price,
                    quantity=remaining_qty,
                    timeInForce='GTC',
                    reduceOnly=True
                )
                logging.info(f"{symbol}: тейк2 обновлён @ {take2_price}, объём {remaining_qty}")


            except Exception as e:

                logging.exception(f"{symbol}: ❌ ошибка при установке нового тейка2 @ {take2_price}, объём {remaining_qty}\n{e}")

                msg = f"{symbol}: ❌ ошибка при установке нового тейка2 @ {take2_price}, объём {remaining_qty}\n{e}"
                await notify_critical_error(msg, key=f"{symbol}_new_take2_error")


        # --- Отмена старого стопа ---
        if old_stop_order and old_stop_order.order_id != new_stop_order['orderId']:
            try:
                await client.cancel_order(symbol=symbol, orderId=old_stop_order.order_id)
                logging.info(f"{symbol}: старый стоп-ордер {old_stop_order.order_id} отменён")


                async with session() as s:
                    old_stop_order.status = 'CANCELED'
                    s.add(old_stop_order)
                    await s.commit()
                    logging.info(f"{symbol}: отменённый стоп-ордер сохранён в БД со статусом CANCELED.")

            except Exception as e:

                    logging.exception(f"{symbol}: ❌ ошибка при отмене старого стопа\n{e}")
                    msg = f"{symbol}: ❌ ошибка при отмене старого стопа\n{e}"
                    await notify_critical_error(msg, key=f"{symbol}_cancel_stop_error")


        # --- Фиксируем флаги ---
        async with session() as s:
            await s.execute(
                update(db.Trades).where(db.Trades.id == trade.id).values(
                    breakeven_stop_price=new_stop,
                    partial_exit_done=True
                )
            )
            await s.commit()
            logging.info(f"{symbol}: флаги обновлены в БД.")

        updated_take_stop[symbol] = (new_stop, take2_price)
        updated_event.set()

    except Exception as e:
        logging.error(f"{symbol}: ошибка после частичного закрытия: {e}")




if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())










