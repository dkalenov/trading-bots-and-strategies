import asyncio
import configparser
import logging
import binance
import db
from datetime import datetime, timezone, timedelta, time
from concurrent.futures import ThreadPoolExecutor
import utils
import traceback
import get_data
import tg
import pprint
from sqlalchemy import select, update

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
session = None

# Потоки для параллельного выполнния задач
executor = ThreadPoolExecutor(max_workers=20)

# все символы для первого запуска
all_symbols: dict[str, binance.SymbolFutures] = {}


all_prices: dict[str, binance.SymbolFutures] = {}

# Символы, по которым в приоритетном порядке собираются сигналы
IMPORTANT_SYMBOLS = ['BTCUSDT', 'ETHUSDT']
VALID_SIGNALS = ['STRONG_BUY', 'STRONG_SELL']

# Первый таймфрейм в приоритете, по нему открываются сделки
timeframes = ["1m", "5m", "15m"]

# сигнал по BTC
btc_signal = None

# список открытых позиций
positions = {}

websockets_list: list[binance.futures.WebsocketAsync] = []

debug = None

symbol_conf_cache: dict[str, db.SymbolsSettings] = {}



async def main():
    global session, conf, client, all_symbols, all_prices, debug, symbol_conf_cache, positions

    config.read('config.ini')
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
        config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()
    client = binance.Futures(
        conf.api_key, conf.api_secret,
        asynced=True, testnet=config.getboolean('BOT', 'testnet')
    )
    debug = config.getboolean('BOT', 'debug')

    # ЗАГРУЖАЕМ КОНФИГИ СИМВОЛОВ ДО подключения к WS
    symbol_confs = await db.get_all_symbols_conf()
    symbol_conf_cache = {s.symbol: s for s in symbol_confs}

    # Загружаем позиции
    await get_data.sync_positions_with_exchange(client, positions)

    open_symbols = [symbol for symbol, status in positions.items() if status]
    print(f"Открытых позиций: {len(open_symbols)}")

    # Загружаем данные о символах и ценах
    all_symbols = await get_data.load_binance_symbols(client)
    all_prices = await get_data.get_all_prices(client)




    symbol_update_lock = asyncio.Lock()

    await asyncio.gather(
        *(timed_collector(tf, symbol_update_lock) for tf in timeframes),
        db.periodic_symbol_update(client, executor, symbol_update_lock, hour=17, minute=35),
        tg.run(),
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
                    await process_main_timeframe_signals()
                else:
                    # await asyncio.sleep(15)
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

    logging.info(f"Результаты сигналов: {results}")

    signals_to_save = [r for r in results if r is not None]
    print('SIGNAL TO SAVE', datetime.now(timezone.utc), signals_to_save)

    if signals_to_save:
        logging.info(f"Сохраняем {len(signals_to_save)} сигналов в БД")
        # await db.save_signals_batch_to_db(signals_to_save)
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
            open_long = recommendation in ['STRONG_BUY', 'BUY']
            open_short = recommendation in ['STRONG_SELL', 'SELL']
        else:
            open_long = recommendation == 'STRONG_BUY' and btc_signal in ['STRONG_BUY', 'BUY', 'NEUTRAL']
            open_short = recommendation == 'STRONG_SELL' and btc_signal in ['STRONG_SELL', 'SELL', 'NEUTRAL']

        signal = None
        if open_long:
            signal = "BUY"
        elif open_short:
            signal = "SELL"

        # print('POSITIONS PROCESS TRADE SIGNAL RUN', datetime.now(timezone.utc), positions)
        if signal and not positions.get(symbol, False):
            logging.info(f"Открытие {signal} по {symbol} @ {entry_price} | BTC = {btc_signal}")
            await new_trade(symbol, interval, signal)

        return signal_to_return

    except Exception as e:
        logging.exception(f"Ошибка при обработке сигнала {symbol}: {e}")
        return signal_to_return




async def new_trade(symbol, interval, signal):
    global positions
    loop = asyncio.get_running_loop()
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



        positions[symbol] = True
        # order_info = await db.get_active_entry_order_info(symbol, client)
        #
        # if order_info:
        #     print("ℹ️ Инфо по входному ордеру:", order_info)





    except:
        print(f"Ошибка при открытии {'ЛОНГОВОЙ' if signal == "BUY" else 'ШОРТОВОЙ'} позиции по {symbol}\n{traceback.format_exc()}")
        positions.pop(symbol, None)
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
                text = (f"Открыл в <b>{'ЛОНГ' if signal=="BUY" else 'ШОРТ'}</b> {quantity} <b>{symbol}</b>\n"
                        f"Цена входа: <b>{entry_price}</b>\n"
                        f"Тейк / стоп: <b>{take_price2} / {stop_price}</b>\n")
                # отправляем сообщение в канал
                msg = await tg.bot.send_message(config['TG']['channel'], text, parse_mode='HTML')
                # записываем идентификатор сообщения в БД
                trade.msg_id = msg.message_id
                await s.commit()
            except Exception as e:
                print(f"Ошибка при отправки сообщения для {symbol}, {e}")
    except:
        print(f"Ошибка при выставлении стопа и тейка по {symbol}\n{traceback.format_exc()}")
        # закрываю сделку по рынку
        await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='MARKET', quantity=quantity,
                               reduceOnly=True)


async def connect_ws():
    global websockets_list, symbol_conf_cache

    logging.info("Подключаемся к вебсокетам...")
    streams = []

    available_symbols = list(symbol_conf_cache.keys())
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in available_symbols if s not in IMPORTANT_SYMBOLS]

    for symbol in symbols_ordered:
        conf = symbol_conf_cache.get(symbol)
        # print(f"WS {symbol}")
        if conf and conf.status:
            # print('STREAMS APPENDED')
            streams.append(f"{symbol.lower()}@kline_{conf.interval}")

    # Разбиваем по чанкам по 100 стримов (лимит Binance)
    chunk_size = 100
    streams_list = [streams[i:i + chunk_size] for i in range(0, len(streams), chunk_size)]

    for stream_chunk in streams_list:
        ws = await client.websocket(stream_chunk, on_message=ws_msg)
        websockets_list.append(ws)


async def ws_msg(ws, msg):
    if 'data' not in msg:
        # print('NO DATA')
        return

    kline = msg['data']['k']
    symbol = kline['s']
    interval = kline['i']
    is_closed = kline['x']  # завершена ли свеча


    try:
        trade = await db.get_open_trade(symbol)

        if trade and not trade.take1_triggered:
            # print("Отслеживаем цену")
            current_price = float(kline['c'])
            direction = "BUY" if trade.side else "SELL"
            take1_price = trade.take1_price

            price_hit = current_price >= take1_price if direction == "BUY" else current_price <= take1_price

            if price_hit:
                async with session() as s:
                    logging.info(f"{symbol}: достигнут цена {take1_price}. Закрытие части и перенос стопа.")
                    take1_triggered = db.Trades(take1_triggered=True)

                s.add(take1_triggered)
                await s.commit()

                asyncio.create_task(partial_close_and_move_stop(trade))
        all_prices[symbol] = float(kline['c'])
        # print(f'WS {symbol} price: {all_prices[symbol]}')
    except Exception as e:
        logging.error(f"Ошибка при обновлении цены {symbol}: {e}")





# async def partial_close_and_move_stop(trade):
#     try:
#         symbol = trade.symbol
#         direction = "BUY" if trade.side else "SELL"
#         close_side = "SELL" if direction == "BUY" else "BUY"
#
#         portion = symbol_conf_cache.get(symbol).portion
#         qty = trade.quantity * portion
#         qty = utils.round_down(qty, all_symbols[symbol].step_size)
#
#         if qty == 0:
#             return
#
#         await client.new_order(
#             symbol=symbol,
#             side=close_side,
#             type='MARKET',
#             quantity=qty,
#             reduceOnly=True
#         )
#
#         logging.info(f"{symbol}: частично закрыл {portion*100}% позиции.")
#
#         # Перенос стопа
#         entry_price = trade.entry_price
#         if direction == "BUY":
#             new_stop = entry_price * 1.001
#         else:
#             new_stop = entry_price * 0.999
#
#         new_stop = round(new_stop, all_symbols[symbol].tick_size)
#
#         await client.new_order(
#             symbol=symbol,
#             side=close_side,
#             type='STOP_MARKET',
#             stopPrice=new_stop,
#             quantity=trade.quantity - qty,
#             reduceOnly=True,
#             timeInForce='GTE_GTC'
#         )
#
#         logging.info(f"{symbol}: стоп перенесён в безубыток ({new_stop}).")
#
#         # Записать в БД новую цену стопа:
#         async with session as s:
#             stmt = update(db.Trades).where(db.Trades.id == trade.id).values(breakeven_stop_price=new_stop)
#             await s.execute(stmt)
#             await s.commit()
#
#     except Exception as e:
#         logging.error(f"Ошибка при частичном закрытии/переносе стопа по {trade.symbol}: {e}")


# Предпоследняя
# async def partial_close_and_move_stop(trade):
#     symbol = trade.symbol
#     try:
#         direction = "BUY" if trade.side else "SELL"
#         close_side = "SELL" if direction == "BUY" else "BUY"
#
#         # Получаем позицию с биржи
#         positions = await client.get_position_risk(symbol=symbol)
#         position_info = next((p for p in positions if p["symbol"] == symbol), None)
#
#         if not position_info:
#             logging.warning(f"{symbol}: позиция не найдена на Binance.")
#             return
#
#         position_amt = float(position_info["positionAmt"])
#
#         if position_amt == 0:
#             logging.warning(f"{symbol}: позиция уже закрыта.")
#             return
#
#         position_side = "BUY" if position_amt > 0 else "SELL"
#         if position_side != direction:
#             logging.warning(f"{symbol}: направление позиции не совпадает с ожидаемым.")
#             return
#
#         position_amt = abs(position_amt)
#         portion = symbol_conf_cache.get(symbol).portion
#         qty = trade.quantity * portion
#         qty = utils.round_down(qty, all_symbols[symbol].step_size)
#
#         if qty == 0 or qty > position_amt:
#             logging.warning(f"{symbol}: невозможно закрыть {qty}, позиция только {position_amt}")
#             return
#
#         # Частичное закрытие
#         try:
#             await client.new_order(
#                 symbol=symbol,
#                 side=close_side,
#                 type='MARKET',
#                 quantity=qty,
#                 reduceOnly=True
#             )
#             logging.info(f"{symbol}: частично закрыл {portion * 100:.1f}% позиции ({qty}).")
#         except Exception as e:
#             logging.error(f"{symbol}: ошибка при MARKET-закрытии части позиции: {e}")
#             return
#
#         # Отмена всех стоп-ордеров
#         try:
#             open_orders = await client.get_open_orders(symbol=symbol)
#             for order in open_orders:
#                 if order['type'] == 'STOP_MARKET':
#                     print('ORDER ID', order['orderId'])
#                     await client.cancel_order(symbol=symbol, orderId=order['orderId'])
#                     logging.info(f"{symbol}: отменён стоп-ордер {order['orderId']}.")
#         except Exception as e:
#             logging.warning(f"{symbol}: не удалось отменить стоп-ордера: {e}")
#
#         # Новый стоп
#         entry_price = trade.entry_price
#         new_stop = entry_price * (1.001 if direction == "BUY" else 0.999)
#         new_stop = round(new_stop, all_symbols[symbol].tick_size)
#
#         remaining_qty = position_amt - qty
#         remaining_qty = utils.round_down(remaining_qty, all_symbols[symbol].step_size)
#
#         if remaining_qty == 0:
#             logging.warning(f"{symbol}: позиция полностью закрыта после частичного выхода.")
#             return
#
#         stop_order_params = {
#             "symbol": symbol,
#             "side": close_side,
#             "type": 'STOP_MARKET',
#             "stopPrice": new_stop,
#             "quantity": remaining_qty,
#             "reduceOnly": True,
#             "timeInForce": 'GTE_GTC'
#         }
#
#         try:
#             await client.new_order(**stop_order_params)
#             logging.info(f"{symbol}: стоп перенесён в безубыток ({new_stop}). Остаток: {remaining_qty}")
#         except Exception as e:
#             error_text = str(e)
#             logging.error(f"{symbol}: ошибка при установке нового стопа:")
#             logging.error(f"→ reduceOnly: True, qty: {remaining_qty}, stop: {new_stop}, side: {close_side}")
#             logging.error(f"→ Ошибка: {error_text}")
#             if "ReduceOnly Order is rejected" in error_text:
#                 logging.error(f"{symbol}: Binance отклонил стоп — возможно, позиция уже закрыта или закрыта вручную.")
#             return
#
#         # Обновление БД
#         try:
#             async with session as s:
#                 stmt = update(db.Trades).where(db.Trades.id == trade.id).values(breakeven_stop_price=new_stop)
#                 await s.execute(stmt)
#                 await s.commit()
#                 logging.info(f"{symbol}: новая цена стопа ({new_stop}) записана в БД.")
#         except Exception as e:
#             logging.error(f"{symbol}: ошибка при обновлении цены стопа в БД: {e}")
#
#     except Exception as e:
#         logging.error(f"{symbol}: критическая ошибка в partial_close_and_move_stop: {e}")






async def partial_close_and_move_stop(trade):
    global positions

    symbol = trade.symbol
    try:
        direction = "BUY" if trade.side else "SELL"
        close_side = "SELL" if direction == "BUY" else "BUY"

        # Получаем позицию с Binance
        binace_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binace_positions if p["symbol"] == symbol), None)

        if not position_info:
            logging.warning(f"{symbol}: позиция не найдена.")
            return

        position_amt = float(position_info["positionAmt"])

        if position_amt == 0:
            logging.warning(f"{symbol}: позиция уже закрыта.")
            return

        position_side = "BUY" if position_amt > 0 else "SELL"
        if position_side != direction:
            logging.warning(f"{symbol}: направление позиции не совпадает с ожидаемым.")
            return

        position_amt = abs(position_amt)
        portion = symbol_conf_cache.get(symbol).portion
        qty = trade.quantity * portion
        qty = utils.round_down(qty, all_symbols[symbol].step_size)

        if qty == 0 or qty > position_amt:
            logging.warning(f"{symbol}: невозможно закрыть {qty}, позиция только {position_amt}")
            return

        # Частичное закрытие позиции
        try:
            await client.new_order(
                symbol=symbol,
                side=close_side,
                type='MARKET',
                quantity=qty,
                reduceOnly=True
            )
            logging.info(f"{symbol}: частично закрыто {portion * 100:.1f}% позиции ({qty})")
        except Exception as e:
            logging.error(f"{symbol}: ошибка при MARKET-закрытии части позиции: {e}")
            return

        # Отмена старых стопов
        try:
            open_orders = await client.get_orders(symbol=symbol)
            print("OPEN ORDERS", open_orders)
            for order in open_orders:
                if order['type'] == 'STOP_MARKET' and order.get('reduceOnly') and order['status'] == 'NEW':
                    await client.cancel_order(symbol=symbol, orderId=order['orderId'])
                    logging.info(f"{symbol}: отменён стоп-ордер {order['orderId']}")
        except Exception as e:
            logging.warning(f"{symbol}: не удалось отменить стоп-ордера: {e}")

        # Повторно проверяем позицию
        binace_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binace_positions if p["symbol"] == symbol), None)
        remaining_amt = abs(float(position_info["positionAmt"]))


        if remaining_amt == 0:
            logging.warning(f"{symbol}: позиция полностью закрыта после частичного выхода.")

            positions[symbol] = False
            return

        # Новый стоп в безубыток
        entry_price = trade.entry_price

        # Для теста Поменял всё местами и увеличил сильно значения
        new_stop = entry_price * (0.993 if direction == "BUY" else 1.007)


        # new_stop = entry_price * (1.001 if direction == "BUY" else 0.999)

        new_stop = round(new_stop, all_symbols[symbol].tick_size)

        remaining_qty = utils.round_down(remaining_amt, all_symbols[symbol].step_size)

        stop_order_params = {
            "symbol": symbol,
            "side": close_side,
            "type": 'STOP_MARKET',
            "stopPrice": new_stop,
            "quantity": remaining_qty,
            "reduceOnly": True
        }

        try:
            await client.new_order(**stop_order_params)
            logging.info(f"{symbol}: новый стоп установлен в безубыток @ {new_stop}, объём {remaining_qty}")
        except Exception as e:
            error_text = str(e)
            logging.error(f"{symbol}: ошибка при установке нового стопа: {error_text}")
            if "ReduceOnly Order is rejected" in error_text:
                logging.error(f"{symbol}: Binance отклонил стоп — возможно, позиция уже закрыта.")
            return

        # Обновляем БД
        try:
            async with session() as s:
                stmt = update(db.Trades).where(db.Trades.id == trade.id).values(breakeven_stop_price=new_stop)
                await s.execute(stmt)
                await s.commit()
                logging.info(f"{symbol}: новая цена стопа ({new_stop}) записана в БД.")
        except Exception as e:
            logging.error(f"{symbol}: ошибка при записи стопа в БД: {e}")

    except Exception as e:
        logging.error(f"{symbol}: критическая ошибка в partial_close_and_move_stop: {e}")






if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())










