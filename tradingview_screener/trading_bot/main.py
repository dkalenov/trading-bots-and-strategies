import asyncio
import configparser
import logging
import datetime
from tradingview_ta import TA_Handler, Interval
from sqlalchemy import update
import binance
import db
from symbols_manager import *
from sqlalchemy import insert
from datetime import datetime, timezone  # для timezone-aware времени
from sqlalchemy.dialects.postgresql import insert


# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
all_symbols: dict[str, binance.SymbolFutures] = {}


IMPORTANT_SYMBOLS = ['BTCUSDT', 'ETHUSDT']
VALID_SIGNALS = ['STRONG_BUY', 'STRONG_SELL']


# Интервалы TradingView
INTERVAL_MAPPING = {
    "1m": Interval.INTERVAL_1_MINUTE,
    "5m": Interval.INTERVAL_5_MINUTES,
    "15m": Interval.INTERVAL_15_MINUTES,
    "30m": Interval.INTERVAL_30_MINUTES,
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
}

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

    while True:
        try:
            symbols = await load_symbols(client)
            # await daily_update_symbols(symbols)
            await collect_signals()
            logging.info("Обновление завершено. Спим 24 часа...")
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}")

        # await asyncio.sleep(86400)  # 24 часа





async def load_symbols(client):
    global all_symbols
    try:
        symbols_data = await client.load_symbols()
        all_symbols = {
            symbol: value for symbol, value in symbols_data.items()
            if symbol.endswith('USDT') and 'USDC' not in symbol
        }
        symbols = [s.symbol for s in all_symbols.values()]
        logging.info(f"Загружены символы: {symbols}")
        await add_new_symbols(symbols)
        return symbols
    except Exception as e:
        logging.error(f"Ошибка при загрузке символов: {e}")
        return []

async def add_new_symbols(symbols):
    async with session() as s:
        try:
            for symbol in symbols:
                stmt = insert(db.Symbols).values(binance_symbol=symbol).on_conflict_do_nothing()
                await s.execute(stmt)
            await s.commit()
        except Exception as e:
            logging.error(f"Ошибка при вставке: {e}")
            await s.rollback()

async def get_tradingview_data(symbol, timeframe, retries=1):
    interval = INTERVAL_MAPPING.get(timeframe, Interval.INTERVAL_4_HOURS)
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
            await asyncio.sleep(1)
    return None



async def daily_update_symbols(symbols, timeframe='4h'):
    async with session() as s:
        try:
            for symbol in symbols:
                data = await get_tradingview_data(symbol, timeframe)
                update_data = {
                        'last_update': datetime.utcnow().replace(second=0, microsecond=0)
                }
                if data:
                    update_data['tradingview_symbol'] = True

                await s.execute(
                    update(db.Symbols)
                    .where(db.Symbols.binance_symbol == symbol)
                    .values(**update_data)
                )
            await s.commit()
        except Exception as e:
            logging.error(f"Ошибка при обновлении символов: {e}")
            await s.rollback()



async def collect_signals(timeframe='4h', max_concurrent_tasks=10):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async with session() as s:
        result = await s.execute(
            select(db.Symbols).where(db.Symbols.tradingview_symbol == True)
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

    async def process_symbol(symbol):
        async with semaphore:
            try:
                data = await get_tradingview_data(symbol, timeframe)
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
                    async with session() as local_session:
                        stmt = insert(db.TradingviewSignals).values(
                            symbol=symbol,
                            interval=timeframe,
                            signal=data['RECOMMENDATION'],
                            entry_price=entry_price,
                            utc_time=datetime.utcnow()
                        )
                        await local_session.execute(stmt)
                        await local_session.commit()
            except Exception as e:
                logging.warning(f"Ошибка сигнала {symbol}: {e}")

    tasks = [process_symbol(symbol) for symbol in symbols_ordered]
    await asyncio.gather(*tasks)






if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())


