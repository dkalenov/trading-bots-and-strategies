import asyncio
import configparser
import logging
from tradingview_ta import TA_Handler, Interval
import binance
import db
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone, timedelta, time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import select, update
import utils
import traceback
import get_data



# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
session = None
executor = ThreadPoolExecutor(max_workers=20)
all_symbols: dict[str, binance.SymbolFutures] = {}
all_prices: dict[str, binance.SymbolFutures] = {}


IMPORTANT_SYMBOLS = ['BTCUSDT', 'ETHUSDT']
VALID_SIGNALS = ['STRONG_BUY', 'STRONG_SELL']

timeframes = ["1m", "5m", "15m"]

btc_signal = None

# список открытых позиций
positions = {}



async def main():
    global session, conf, client, all_symbols, all_prices

    config.read('config.ini')
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
        config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()
    client = binance.Futures(
        conf.api_key, conf.api_secret,
        asynced=True, testnet=config.getboolean('BOT', 'testnet')
    )

    # Проверка: есть ли символы с поддержкой TradingView

    # Запуск сборщиков сигналов и обновления символов

    all_symbols = await get_data.load_binance_symbols(client)
    all_prices = await get_data.get_all_prices(client)
    # print(f" ALL SYMBOLS {all_symbols}")
    # asyncio.create_task(load_binance_symbols(client))

    symbol_update_lock = asyncio.Lock()

    await asyncio.gather(
        *(timed_collector(tf, symbol_update_lock) for tf in timeframes),
        db.periodic_symbol_update(client, executor, symbol_update_lock, hour=17, minute=35)
    )



async def timed_collector(timeframe: str, lock: asyncio.Lock):
    while True:
        await utils.wait_for_next_candle(timeframe)
        async with lock:
            try:
                if timeframe == timeframes[0]:
                    await process_main_timeframe_signals()
                else:
                    await asyncio.sleep(10)
                    await collect_signals(timeframe)
            except Exception as e:
                logging.error(f"[{timeframe}] Ошибка в сборщике сигналов: {e}")



async def collect_signals(timeframe=timeframes[0]):
    available_symbols = await db.get_all_available_symbols()
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in available_symbols if s not in IMPORTANT_SYMBOLS]

    loop = asyncio.get_running_loop()
    tasks = []

    for symbol in symbols_ordered:
        tasks.append(process_symbol(symbol, timeframe, loop))
    await asyncio.gather(*tasks)



async def process_symbol(symbol, interval, loop):
    data = await loop.run_in_executor(executor, get_data.get_tradingview_data, symbol, interval)
    if not data:
        return

    entry_price = all_prices.get(symbol)
    if entry_price is None:
        logging.warning(f"Цена не найдена для {symbol}")
        return

    # is_important = symbol in IMPORTANT_SYMBOLS
    # is_valid = data['RECOMMENDATION'] in VALID_SIGNALS
    #
    # if is_important or is_valid:
    #     logging.info(f"Сигнал {symbol}: {data['RECOMMENDATION']} по цене {entry_price}")

    await db.save_signal_to_db(symbol, interval, data['RECOMMENDATION'], entry_price)



async def process_main_timeframe_signals():
    global btc_signal
    logging.info(f"Обработка сигналов {timeframes[0]} и открытие сделок...")

    interval = timeframes[0]

    # Сначала обрабатываем BTC отдельно
    loop = asyncio.get_running_loop()
    btc_data = await loop.run_in_executor(executor, get_data.get_tradingview_data, 'BTCUSDT', interval)
    if not btc_data:
        logging.error("Не удалось получить сигнал BTCUSDT — пропускаем обработку.")
        return
    btc_signal = btc_data['RECOMMENDATION']
    logging.info(f"Сигнал BTCUSDT {timeframes[0]}: {btc_signal}")

    available_symbols = await db.get_all_available_symbols()
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in available_symbols if s not in IMPORTANT_SYMBOLS]

    tasks = []
    for symbol in symbols_ordered:
        tasks.append(process_trade_signal(symbol, interval))
    await asyncio.gather(*tasks)



async def process_trade_signal(symbol, interval):
    signal = None
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(executor, get_data.get_tradingview_data, symbol, interval)
    if not data:
        return

    entry_price = all_prices.get(symbol)
    # print(f"{symbol} ENTRY PRICE: {entry_price}")
    if entry_price is None:
        logging.warning(f"Цена не найдена для {symbol}")
        return

    recommendation = data['RECOMMENDATION']
    is_important = symbol in IMPORTANT_SYMBOLS
    is_valid = recommendation in VALID_SIGNALS

    # Сохраняем сигнал независимо от открытия сделки
    await db.save_signal_to_db(symbol, interval, recommendation, entry_price)

    # Логика открытия сделки
    open_long = recommendation == 'STRONG_BUY' and btc_signal in ['STRONG_BUY', 'BUY', 'NEUTRAL']
    open_short = recommendation == 'STRONG_SELL' and btc_signal in ['STRONG_SELL', 'SELL', 'NEUTRAL']

    if open_long:
        signal = "BUY"
    elif open_short:
        signal = "SELL"

    if is_important or is_valid:
        if signal:
            logging.info(f"Открытие {'LONG' if open_long else 'SHORT'} по {symbol} @ {entry_price} | BTC = {btc_signal}")

            await new_trade(symbol, interval, signal)



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
            return

        #получаем настройки для символа
        try:
            symbol_conf = await db.get_symbol_conf(symbol)
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
        # last_price = float((await client.ticker_price(symbol))['price'])
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
        entry_order = await client.new_order(symbol=symbol, side='BUY' if signal == "BUY" else 'SELL', type='MARKET',
                                             quantity=quantity, newOrderRespType='RESULT')


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
            take_price = entry_price + atr * symbol_conf.take
            stop_price = entry_price - atr * symbol_conf.stop
        else:
            take_price = entry_price - atr * symbol_conf.take
            stop_price = entry_price + atr * symbol_conf.stop

        # округляем их
        take_price = round(take_price, symbol_info.tick_size)
        stop_price = round(stop_price, symbol_info.tick_size)
        # создаем ордеры на стоп и тейк
        stop_order = await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='STOP_MARKET',
                                            quantity=quantity, stopPrice=stop_price, timeInForce='GTE_GTC', reduceOnly=True)
        take_order = await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='LIMIT',
                                            quantity=quantity, price=take_price, timeInForce='GTC', reduceOnly=True)
        print(f"Открыли сделку по {symbol} по цене {entry_price} с тейком {take_price} и стопом {stop_price}")
        # создаем сессию для работы с базой данных
        async with session() as s:
            # записывает сделку в БД
            trade = db.Trades(symbol=symbol, order_size=float(entry_order['cumQuote']), side = "BUY" if signal == "BUY" else "SELL",
                              status='NEW', open_time=entry_order['updateTime'], interval=interval, leverage=symbol_conf.leverage,
                              atr_length=atr_length, atr=atr, entry_price=entry_price, quantity=quantity, take1_price=take_price,
                              take2_price=take_price, stop_price=stop_price)



            s.add(trade)
            # отправляем данные в БД
            await s.commit()
            # записываем ордера в БД
            for order in (entry_order, stop_order, take_order):
                s.add(db.Orders(order_id=order['orderId'], trade_id=trade.id, symbol=symbol, time=order['updateTime'],
                                side=order['side'] == 'BUY', type=order['type'], status=order['status'],
                                reduce=order['reduceOnly'], price=float(order['avgPrice']),
                                quantity=float(order['executedQty'])))
    #         # отправляем данные в БД
    #         await s.commit()
            # формируем текст поста в канал
            # text = (f"Открыл в <b>{'ЛОНГ' if signal.side else 'ШОРТ'}</b> {quantity} <b>{symbol}</b>\n"
            #         f"Цена входа: <b>{entry_price}</b>\n"
            #         f"Тейк / стоп: <b>{take_price} / {stop_price}</b>\n")
            # # отправляем сообщение в канал
            # msg = await tg.bot.send_message(config['TG']['channel'], text, parse_mode='HTML')
            # # записываем идентификатор сообщения в БД
            # trade.msg_id = msg.message_id
            # await s.commit()
    except:
        print(f"Ошибка при выставлении стопа и тейка по {symbol}\n{traceback.format_exc()}")
        # закрываю сделку по рынку
        await client.new_order(symbol=symbol, side='SELL' if signal == "BUY" else 'BUY', type='MARKET', quantity=quantity,
                               reduceOnly=True)









if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())

