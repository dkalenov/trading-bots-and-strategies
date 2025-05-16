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




# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
session = None
executor = ThreadPoolExecutor(max_workers=20)
all_symbols: dict[str, binance.SymbolFutures] = {}

IMPORTANT_SYMBOLS = ['BTCUSDT', 'ETHUSDT']
VALID_SIGNALS = ['STRONG_BUY', 'STRONG_SELL']

INTERVAL_MAPPING = {
    "1m": Interval.INTERVAL_1_MINUTE,
    "5m": Interval.INTERVAL_5_MINUTES,
    "15m": Interval.INTERVAL_15_MINUTES,
    "30m": Interval.INTERVAL_30_MINUTES,
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
}


timeframes = ["1m", "5m", "15m"]

btc_signal = None

# список открытых позиций
positions = {}


async def main():
    global session, conf, client

    config.read('config.ini')
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
        config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()
    client = binance.Futures(
        conf.api_key, conf.api_secret,
        asynced=True, testnet=config.getboolean('BOT', 'testnet')
    )

    # Проверка: есть ли символы с поддержкой TradingView
    if await db.is_symbols_table_empty():
        logging.info("Символы с поддержкой TradingView не найдены — загружаем.")
        await daily_update_symbols()

        # Повторная проверка после обновления
        if await db.is_symbols_table_empty():
            logging.error("После обновления не найдено ни одного подходящего символа. Завершаем.")
            return
        else:
            logging.info("Символы успешно загружены после обновления.")
    else:
        logging.info("Символы уже есть. Продолжаем.")

    # Запуск сборщиков сигналов и обновления символов

    asyncio.create_task(load_binance_symbols(client))

    await asyncio.gather(
        *(timed_collector(tf) for tf in timeframes),
        periodic_symbol_update()
    )





async def load_binance_symbols(client):
    global all_symbols
    try:
        symbols_data = await client.load_symbols()
        all_symbols = {
            symbol: value for symbol, value in symbols_data.items()
            if symbol.endswith('USDT') and 'USDC' not in symbol
        }
        symbols = [s.symbol for s in all_symbols.values()]
        logging.info(f"Загружены символы: {symbols}")
        await db.update_binance_symbols_db(symbols)
    except Exception as e:
        logging.error(f"Ошибка при загрузке символов: {e}")

async def timed_collector(timeframe: str):
    while True:
        await utils.wait_for_next_candle(timeframe)
        try:
            if timeframe == timeframes[0]:
                await process_main_timeframe_signals()
            else:
                await asyncio.sleep(10) # ждём чтобы основной поток успел завершиться
                await collect_signals(timeframe)
        except Exception as e:
            logging.error(f"[{timeframe}] Ошибка в сборщике сигналов: {e}")


async def daily_update_symbols():
    global all_symbols
    try:
        logging.info("Запуск ежедневного обновления символов...")

        # 1. Загружаем символы с Binance и обновляем БД (используем существующую функцию)
        await db.load_binance_symbols(client)

        # 2. Проверяем, какие из них доступны в TradingView
        binance_symbols = list(all_symbols.keys())
        loop = asyncio.get_running_loop()
        results = await asyncio.gather(
            *[loop.run_in_executor(executor, db.is_tradingview_symbols_available, symbol) for symbol in binance_symbols]
        )

        # 3. Обновляем флаги tradingview_symbol
        async with session() as s:
            try:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                for symbol, available in zip(binance_symbols, results):
                    stmt = (
                        update(db.Symbols)
                        .where(db.Symbols.binance_symbol == symbol)
                        .values(tradingview_symbol=available, last_update=now)
                    )
                    await s.execute(stmt)
                await s.commit()
                logging.info("Флаги tradingview обновлены.")
            except Exception as e:
                await s.rollback()
                logging.error(f"Ошибка при обновлении tradingview флагов: {e}")

    except Exception as e:
        logging.error(f"Ошибка в daily_update_symbols: {e}")





async def process_main_timeframe_signals():
    global btc_signal
    logging.info(f"Обработка сигналов {timeframes[0]} и открытие сделок...")

    interval = INTERVAL_MAPPING[timeframes[0]]
    async with session() as s:
        result = await s.execute(
            select(db.Symbols).where(db.Symbols.tradingview_symbol.is_(True))
        )
        symbols = result.scalars().all()
        symbol_names = [s.binance_symbol for s in symbols]

    try:
        prices_raw = await client.mark_price()
        prices = {item['symbol']: float(item['markPrice']) for item in prices_raw}
    except Exception as e:
        logging.error(f"Ошибка при получении цен: {e}")
        return

    # Сначала обрабатываем BTC отдельно
    loop = asyncio.get_running_loop()
    btc_data = await loop.run_in_executor(executor, get_tradingview_data, 'BTCUSDT', interval)
    if not btc_data:
        logging.error("Не удалось получить сигнал BTCUSDT — пропускаем обработку.")
        return
    btc_signal = btc_data['RECOMMENDATION']
    logging.info(f"Сигнал BTCUSDT {timeframes[0]}: {btc_signal}")

    # Обрабатываем все остальные сигналы, включая ETH
    other_symbols = [s for s in symbol_names]
    symbols_ordered = IMPORTANT_SYMBOLS + [s for s in other_symbols if s not in IMPORTANT_SYMBOLS]

    tasks = []
    for symbol in symbols_ordered:
        tasks.append(process_trade_signal(symbol, interval, prices))

    await asyncio.gather(*tasks)



async def process_trade_signal(symbol, interval, prices):
    signal = None
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(executor, get_tradingview_data, symbol, interval)
    if not data:
        return

    entry_price = prices.get(symbol)
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

        # try:
            # async with session() as s:
                # Проверяем, есть ли настройки для этого символа
        #         result = await s.execute(
        #             select(db.SymbolsSettings).where(db.SymbolsSettings.symbol == symbol)
        #         )
        #         settings = result.scalar_one_or_none()
        #
        #         if not settings:
        #             # Проверяем, что символ есть в таблице symbols и он поддерживается TradingView
        #             result = await s.execute(
        #                 select(db.Symbols).where(
        #                     db.Symbols.binance_symbol == symbol,
        #                     db.Symbols.tradingview_symbol == True
        #                 )
        #             )
        #             symbol_exists = result.scalar_one_or_none()
        #
        #             if symbol_exists:
        #                 # Добавляем настройки по умолчанию
        #                 s.add(db.SymbolsSettings(symbol=symbol))
        #                 await s.commit()
        #                 logging.info(f"Добавлены настройки по умолчанию для {symbol}.")
        #
        #                 # Повторно получаем настройки, чтобы достать значения по умолчанию из БД
        #                 result = await s.execute(
        #                     select(db.SymbolsSettings).where(db.SymbolsSettings.symbol == symbol)
        #                 )
        #                 settings = result.scalar_one()
        #             else:
        #                 logging.error(f"Символ {symbol} не найден в таблице symbols или не поддерживается TradingView.")
        #                 return
        #
        #         # Получаем параметры из settings (уже гарантированно существуют)
        #         atr_length = settings.atr_length
        #
        #
        # except Exception as e:
        #     logging.error(f"Ошибка при получении данных из настройки сивола: {e}")
        #     return




        # получаем информацию о символе
        if not (symbol_info := all_symbols.get(symbol)):
            print("RETURN")
            return

        #получаем настройки для символа
        try:
            symbol_conf = await db.get_symbol_conf(symbol)
            print(f"{symbol}: {symbol_conf}")
        except Exception as e:
            logging.error(f"Ошибка при получении конфигурации символа {symbol}: {e}")
            return

        atr_length = symbol_conf.atr_length
        print(f"{symbol} atr: {atr_length}")
        try:
            df = await loop.run_in_executor(executor, utils.calculate_atr, klines, atr_length)
        except Exception as e:
            logging.error(f"Ошибка при расчёте atr: {e}")
            return

        last_row = df.iloc[-1]
        atr = float(last_row["ATR"])
        # print(f'{symbol}, atr:{atr}')

        #получаем последнюю цену
        last_price = float((await client.ticker_price(symbol))['price'])
        print(f"{symbol}: {last_price}")
        # расчитываем количество
        quantity = utils.round_down(symbol_conf.order_size / last_price, symbol_info.step_size)
        print(f"{symbol}: {quantity}")

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
            # отправляем данные в БД
            await s.commit()
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


async def periodic_symbol_update(hour=17, minute=30):
    while True:
        now = datetime.now(timezone.utc)
        target_time = datetime.combine(now.date(), time(hour, minute)).replace(tzinfo=timezone.utc)

        # Если сейчас уже позже 05:30 — переносим на следующий день
        if now >= target_time:
            target_time += timedelta(days=1)

        wait_seconds = (target_time - now).total_seconds()
        logging.info(f"Ждём {int(wait_seconds)} секунд до следующего обновления символов в {hour}:{minute} UTC...")
        await asyncio.sleep(wait_seconds)

        await daily_update_symbols()



def get_tradingview_data(symbol, timeframe, retries=3):

    interval = INTERVAL_MAPPING[timeframe]
    for attempt in range(retries):
        try:
            handler = TA_Handler(
                symbol=symbol,
                exchange='Binance',
                screener="crypto",
                interval=interval
            )
            analysis = handler.get_analysis().summary
            analysis['SYMBOL'] = symbol
            return analysis
        except Exception as e:
            logging.warning(f"TV ошибка {symbol} ({timeframe}), попытка {attempt + 1}: {e}")
            # if attempt < retries - 1:
            #     time.sleep(0.5)
    return None



async def collect_signals(timeframe=timeframes[0]):
    interval = INTERVAL_MAPPING[timeframe]

    async with session() as s:
        result = await s.execute(
            select(db.Symbols).where(db.Symbols.tradingview_symbol.is_(True))
        )
        symbols = result.scalars().all()
        symbol_names = [s.binance_symbol for s in symbols]

    try:
        prices_raw = await client.mark_price()
        prices = {item['symbol']: float(item['markPrice']) for item in prices_raw}
    except Exception as e:
        logging.error(f"Ошибка при получении mark_price: {e}")
        return

    other_symbols = [s for s in symbol_names if s not in IMPORTANT_SYMBOLS]
    symbols_ordered = IMPORTANT_SYMBOLS + other_symbols

    loop = asyncio.get_running_loop()
    tasks = []

    for symbol in symbols_ordered:
        tasks.append(process_symbol(symbol, interval, timeframe, prices, loop))

    await asyncio.gather(*tasks)


async def process_symbol(symbol, interval, timeframe, prices, loop):
    data = await loop.run_in_executor(executor, get_tradingview_data, symbol, interval)
    if not data:
        return

    entry_price = prices.get(symbol)
    if entry_price is None:
        logging.warning(f"Цена не найдена для {symbol}")
        return

    # is_important = symbol in IMPORTANT_SYMBOLS
    # is_valid = data['RECOMMENDATION'] in VALID_SIGNALS
    #
    # if is_important or is_valid:
    #     logging.info(f"Сигнал {symbol}: {data['RECOMMENDATION']} по цене {entry_price}")


    await db.save_signal_to_db(symbol, timeframe, data['RECOMMENDATION'], entry_price)




if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())

