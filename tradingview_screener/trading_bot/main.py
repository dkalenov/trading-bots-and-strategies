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
executor = ThreadPoolExecutor(max_workers=30)
all_symbols: dict[str, binance.SymbolFutures] = {}
all_prices: dict[str, binance.SymbolFutures] = {}


IMPORTANT_SYMBOLS = ['BTCUSDT', 'ETHUSDT']
VALID_SIGNALS = ['STRONG_BUY', 'STRONG_SELL']

# timeframes = ["15m", "5m", "30m"]
timeframes = ["5m"]

btc_signal = None

# список открытых позиций
positions = {}

websockets_list: list[binance.futures.WebsocketAsync] = []

debug = None

symbol_conf_cache: dict[str, db.SymbolsSettings] = {}

in_progress = {}


from collections import defaultdict
import asyncio

symbol_locks = defaultdict(asyncio.Lock)



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
                text = (f"Открыл в <b>{'ЛОНГ' if signal == 'BUY' else 'ШОРТ'}</b> {quantity} <b>{symbol}</b>\n"
                        f"Цена входа: <b>{entry_price}</b>\n"
                        f"Тейк 1: <b>{take_price1}</b>\n"
                        f"Тейк 2: <b>{take_price2}</b>\n"
                        f"Стоп: <b>{stop_price}</b>\n"
                        )
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


from datetime import datetime

async def ws_msg(ws, msg):
    if 'data' not in msg:
        return

    kline = msg['data']['k']
    symbol = kline['s']
    interval = kline['i']
    is_closed = kline['x']
    current_price = float(kline['c'])

    try:
        trade = await db.get_open_trade(symbol)
        if not trade:
            return

        direction = "BUY" if trade.side else "SELL"

        take1_hit = current_price >= trade.take1_price if direction == "BUY" else current_price <= trade.take1_price
        take2_hit = current_price >= trade.take2_price if direction == "BUY" else current_price <= trade.take2_price
        stop_hit = current_price <= trade.stop_price if direction == "BUY" else current_price >= trade.stop_price

        async with session() as s:

            # ☑️ Частичное закрытие по Take1 и перенос стопа
            if not trade.take1_triggered and take1_hit:
                logging.info(f"{symbol}: достигнут Take1 {trade.take1_price}. Частичное закрытие и перенос стопа.")

                stmt = (
                    update(db.Trades)
                    .where(db.Trades.id == trade.id)
                    .values(take1_triggered=True)
                )
                await s.execute(stmt)
                await s.commit()

                asyncio.create_task(partial_close_and_move_stop(trade))
                return

            # ✅ Полное закрытие по Take2 или Stop (НЕ зависит от take1)
            if take2_hit or stop_hit:
                reason = "🎯 Закрытие по TAKE2" if take2_hit else "⛔️ Закрытие по СТОПУ"
                close_price = trade.take2_price if take2_hit else trade.stop_price

                logging.info(f"{symbol}: {reason} по цене {close_price}")

                stmt = (
                    update(db.Trades)
                    .where(db.Trades.id == trade.id)
                    .values(
                        position_open=False,
                        close_time=int(datetime.utcnow().timestamp() * 1000),
                        status='TAKE2' if take2_hit else 'STOP1'
                    )
                )
                await s.execute(stmt)
                await s.commit()

                try:
                    text = (
                        f"<b>{symbol}</b> {reason}\n"
                        f"Цена закрытия: <b>{close_price}</b>"
                    )
                    if trade.msg_id:
                        await tg.bot.send_message(config['TG']['channel'], text, parse_mode='HTML')
                except Exception as e:
                    logging.error(f"Ошибка при отправке сообщения о закрытии {symbol}: {e}")

                positions.pop(symbol, None)
                return


            if trade.breakeven_stop_price:
                close_breakeven_hit = current_price <= trade.breakeven_stop_price if direction == "BUY" else current_price >= trade.breakeven_stop_price

                if close_breakeven_hit:

                    reason = "🎯 Закрылись в БУ"
                    close_breakeven= trade.breakeven_stop_price

                    logging.info(f"{symbol}: {reason} по цене {close_breakeven}")

                    stmt = (
                        update(db.Trades)
                        .where(db.Trades.id == trade.id)
                        .values(
                            position_open=False,
                            close_time=int(datetime.utcnow().timestamp() * 1000),
                            status='STOP2'
                        )
                    )
                    await s.execute(stmt)
                    await s.commit()

                    try:
                        text = (
                            f"<b>{symbol}</b> {reason}\n"
                            f"Цена закрытия: <b>{close_breakeven}</b>"
                        )
                        if trade.msg_id:
                            await tg.bot.send_message(config['TG']['channel'], text, parse_mode='HTML')
                    except Exception as e:
                        logging.error(f"Ошибка при отправке сообщения о закрытии {symbol}: {e}")

                    positions.pop(symbol, None)
                    return



    except Exception as e:
        logging.error(f"Ошибка при обработке цены {symbol}: {e}")





# async def ws_msg(ws, msg):
#     if 'data' not in msg:
#         # print('NO DATA')
#         return
#
#     kline = msg['data']['k']
#     symbol = kline['s']
#     interval = kline['i']
#     is_closed = kline['x']  # завершена ли свеча
#
#
#     try:
#         trade = await db.get_open_trade(symbol)
#
#         if trade and not trade.take1_triggered:
#             # print("Отслеживаем цену")
#             current_price = float(kline['c'])
#             direction = "BUY" if trade.side else "SELL"
#             take1_price = trade.take1_price
#
#             price_hit = current_price >= take1_price if direction == "BUY" else current_price <= take1_price
#
#             if price_hit:
#
#                 async with session() as s:
#                     logging.info(f"{symbol}: достигнут цена {take1_price}. Закрытие части и перенос стопа.")
#
#                     stmt = (
#                         update(db.Trades)
#                         .where(db.Trades.id == trade.id)
#                         .values(take1_triggered=True)
#                     )
#                     await s.execute(stmt)
#                     await s.commit()
#
#                 asyncio.create_task(partial_close_and_move_stop(trade))
#         # all_prices[symbol] = float(kline['c'])
#         # print(f'WS {symbol} price: {all_prices[symbol]}')
#
#
#     except Exception as e:
#         logging.error(f"Ошибка при обновлении цены {symbol}: {e}")


async def partial_close_and_move_stop(trade):
    global positions

    close_portion = False
    move_stop = False

    portion_close_order = None

    msgs = []

    symbol = trade.symbol

    if trade.partial_exit_done:
        logging.info(f"{symbol}: частичное закрытие уже выполнено ранее, пропуск.")
        return

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


        # Частичное закрытие позиции
        try:
            if qty == 0 or qty > position_amt:
                    logging.warning(f"{symbol}: невозможно закрыть {qty}, позиция только {position_amt}")
            else:
                portion_close_order = await client.new_order(
                symbol=symbol,
                side=close_side,
                type='MARKET',
                quantity=qty,
                reduceOnly=True
            )
            logging.info(f"{symbol}: частично закрыто {portion * 100:.1f}% позиции ({qty})")

            msg = f"{symbol}: частично закрыто {portion * 100:.1f}% позиции ({qty})"
            msgs.append(msg)


            # Обновляем флаг частичного выхода
            try:
                async with session() as s:
                    stmt = update(db.Trades).where(db.Trades.id == trade.id).values(partial_exit_done=True)
                    await s.execute(stmt)
                    await s.commit()
                    logging.info(f"{symbol}: флаг partial_exit_done установлен.")



                    s.add(db.Orders(order_id=portion_close_order['orderId'], trade_id=trade.id, symbol=symbol, time=portion_close_order['updateTime'],
                                    side=portion_close_order['side'] == 'BUY', type=portion_close_order['type'], status=portion_close_order['status'],
                                    reduce=portion_close_order['reduceOnly'], price=float(portion_close_order['avgPrice']),
                                    quantity=float(portion_close_order['executedQty'])))
                    # отправляем данные в БД
                    await s.commit()
                    logging.info(f"{symbol}: portion close order сохранён в бд.")

            except Exception as e:
                logging.error(f"{symbol}: ошибка при обновлении partial_exit_done в БД: {e}")



        except Exception as e:
            logging.error(f"{symbol}: ошибка при MARKET-закрытии части позиции: {e}")

            msg = f"{symbol}: ошибка при MARKET-закрытии части позиции: {e}"
            msgs.append(msg)



            # Отмена старых стопов
        try:
            # Повторно проверяем позицию
            binace_positions = await client.get_position_risk(symbol=symbol)
            position_info = next((p for p in binace_positions if p["symbol"] == symbol), None)
            remaining_amt = abs(float(position_info["positionAmt"]))

            if remaining_amt == 0:
                logging.warning(f"{symbol}: позиция полностью закрыта после частичного выхода.")
                msg = f"{symbol}: позиция полностью закрыта после частичного выхода."
                msgs.append(msg)

                positions[symbol] = False
                return

            # Новый стоп в безубыток
            entry_price = trade.entry_price

            # Для теста Поменял всё местами и увеличил сильно значения
            new_stop = entry_price * (0.999 if direction == "BUY" else 1.001)

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


            new_stop_order = await client.new_order(**stop_order_params)
            logging.info(f"{symbol}: новый стоп установлен в безубыток @ {new_stop}, объём {remaining_qty}")

            msg = f"{symbol}: новый стоп установлен в безубыток @ {new_stop}, объём {remaining_qty}"
            msgs.append(msg)


        except Exception as e:
            error_text = str(e)
            logging.error(f"{symbol}: ошибка при установке нового стопа: {error_text}")

            msg = f"{symbol}: ошибка при установке нового стопа: {error_text}"
            msgs.append(msg)

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

                s.add(db.Orders(order_id=new_stop_order['orderId'], trade_id=trade.id, symbol=symbol,
                                time=new_stop_order['updateTime'],
                                side=new_stop_order['side'] == 'BUY', type=new_stop_order['type'],
                                status=new_stop_order['status'],
                                reduce=new_stop_order['reduceOnly'], price=float(new_stop_order['avgPrice']),
                                quantity=float(new_stop_order['executedQty'])))
                # отправляем данные в БД
                await s.commit()
                logging.info(f"{symbol}: new_stop_order сохранён в бд.")

        except Exception as e:
            logging.error(f"{symbol}: ошибка при записи стопа в БД: {e}")



            if new_stop_order:
                try:
                    open_orders = await client.get_orders(symbol=symbol)
                    # print("OPEN ORDERS", open_orders)
                    for order in open_orders:
                        if order['type'] == 'STOP_MARKET' and order.get('reduceOnly') and order['status'] == 'NEW':
                            cancel_order = await client.cancel_order(symbol=symbol, orderId=order['orderId'])
                            logging.info(f"{symbol}: отменён стоп-ордер {order['orderId']}")
                            # print('CANCEL ORDER', cancel_order)

                            try:
                                async with session() as s:
                                    s.add(db.Orders(order_id=cancel_order['orderId'], trade_id=trade.id, symbol=symbol,
                                                    time=cancel_order['updateTime'],
                                                    side=cancel_order['side'] == 'BUY', type=cancel_order['type'],
                                                    status=cancel_order['status'],
                                                    reduce=cancel_order['reduceOnly'],
                                                    price=float(cancel_order['avgPrice']),
                                                    quantity=float(cancel_order['executedQty'])))
                                    # отправляем данные в БД
                                    await s.commit()
                                    logging.info(f"{symbol}: portion close order сохранён в бд.")

                            except Exception as e:
                                logging.error(f"{symbol}: ошибка при обновлении partial_exit_done в БД: {e}")

                except Exception as e:
                    logging.warning(f"{symbol}: не удалось отменить стоп-ордера: {e}")


    except Exception as e:
        logging.error(f"{symbol}: критическая ошибка в partial_close_and_move_stop: {e}")

        msg = f"{symbol}: критическая ошибка в partial_close_and_move_stop: {e}"
        msgs.append(msg)


    try:

        text_parts = [f"<b>{symbol}</b> — частичное закрытие и перенос стопа\n"]

        # # Добавим сообщения, если были
        if msgs:
            text_parts.append("\n".join(msgs))

        text = '\n'.join(text_parts)

        msg = await tg.bot.send_message(config['TG']['channel'], text, parse_mode='HTML')

        if msg:
            async with session() as s:
                stmt = update(db.Trades).where(db.Trades.id == trade.id).values(msg_id=msg.message_id)
                await s.execute(stmt)
                await s.commit()
            logging.info(f"{symbol}: сообщение отправлено в Telegram")
        else:
            logging.warning(f"{symbol}: сообщение не отправлено, msg = None")


    except Exception as e:
        logging.error(f"{symbol}: ошибка при отправке сообщения в Telegram: {e}")



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())










