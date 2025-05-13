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


# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
all_symbols: dict[str, binance.SymbolFutures] = {}
session = None
executor = ThreadPoolExecutor(max_workers=20)

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

timeframes = ["1m", "5m"]

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
    if await is_symbols_table_empty():
        logging.info("Символы с поддержкой TradingView не найдены — загружаем.")
        await daily_update_symbols()

        # Повторная проверка после обновления
        if await is_symbols_table_empty():
            logging.error("После обновления не найдено ни одного подходящего символа. Завершаем.")
            return
        else:
            logging.info("Символы успешно загружены после обновления.")
    else:
        logging.info("Символы уже есть. Продолжаем.")

    # Запуск сборщиков сигналов и обновления символов

    await asyncio.gather(
        *(timed_collector(tf) for tf in timeframes),
        periodic_symbol_update()
    )


async def is_symbols_table_empty():
    async with session() as s:
        result = await s.execute(
            select(db.Symbols.binance_symbol)
            .where(db.Symbols.tradingview_symbol.is_(True))
            .limit(1)
        )
        return result.scalar_one_or_none() is None



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


async def daily_update_symbols():
    global all_symbols

    try:
        logging.info("Запуск ежедневного обновления символов...")

        # 1. Загружаем символы с Binance и обновляем БД (используем существующую функцию)
        await load_binance_symbols(client)

        # 2. Проверяем, какие из них доступны в TradingView
        binance_symbols = list(all_symbols.keys())
        loop = asyncio.get_running_loop()
        results = await asyncio.gather(
            *[loop.run_in_executor(executor, utils.is_tradingview_symbols_available, symbol) for symbol in binance_symbols]
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




async def timed_collector(timeframe: str):
    while True:
        await utils.wait_for_next_candle(timeframe)
        try:
            await collect_signals(timeframe)
        except Exception as e:
            logging.error(f"[{timeframe}] Ошибка в сборщике сигналов: {e}")



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
        await update_binance_symbols_db(symbols)
    except Exception as e:
        logging.error(f"Ошибка при загрузке символов: {e}")


async def update_binance_symbols_db(symbols):
    async with session() as s:
        try:
            for symbol in symbols:
                stmt = insert(db.Symbols).values(binance_symbol=symbol).on_conflict_do_nothing()
                await s.execute(stmt)
            await s.commit()
        except Exception as e:
            logging.error(f"Ошибка при вставке: {e}")
            await s.rollback()



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
            if attempt < retries - 1:
                time.sleep(0.5)
    return None



async def collect_signals(timeframe='4h'):
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

    is_important = symbol in IMPORTANT_SYMBOLS
    is_valid = data['RECOMMENDATION'] in VALID_SIGNALS

    if is_important or is_valid:
        logging.info(f"Сигнал {symbol}: {data['RECOMMENDATION']} по цене {entry_price}")

        async with session() as s_local:
            try:
                stmt = insert(db.TradingviewSignals).values(
                    symbol=symbol,
                    interval=timeframe,
                    signal=data['RECOMMENDATION'],
                    entry_price=entry_price,
                    utc_time=datetime.now(timezone.utc)
                )
                await s_local.execute(stmt)
                await s_local.commit()
            except Exception as db_e:
                logging.error(f"Ошибка при сохранении сигнала {symbol}: {db_e}")
                await s_local.rollback()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())

