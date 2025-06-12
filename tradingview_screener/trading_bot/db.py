from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, Float, Boolean, DateTime, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import logging
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone, timedelta, time
from sqlalchemy import select, update
from tradingview_ta import TA_Handler, Interval
import binance
import asyncio
import get_data


# базовый класс для определения моделей базы данных, из него будут унаследованы структуры таблиц в бд
Base = declarative_base()

# класс для асинхронных операций с базой данных
Session: async_sessionmaker
client: binance.Futures




class Config(Base):
    __tablename__ = 'config'
    key = Column(String, primary_key=True)
    value = Column(String)

class TradingviewSignals(Base):
    __tablename__ = 'tradingview_signals'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    interval = Column(String)
    signal = Column(String)
    entry_price = Column(Float)
    utc_time = Column(DateTime(timezone=True))

class Trades(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    position_open = Column(Boolean, default=True)
    interval = Column(String, default='4h')
    order_size = Column(Float)
    side = Column(Boolean)
    status = Column(String)
    leverage = Column(Integer)
    atr_length = Column(Integer)
    atr = Column(Float)
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    entry_price = Column(Float)
    quantity = Column(Float)
    take1_price = Column(Float)
    take2_price = Column(Float)
    stop_price = Column(Float)
    breakeven_stop_price = Column(Float)
    take1_triggered = Column(Boolean, default=False)
    result = Column(Float)
    msg_id = Column(Integer)


class Orders(Base):
    __tablename__ = 'orders'
    order_id = Column(BigInteger, primary_key=True, autoincrement=True)
    trade_id = Column(Integer)
    symbol = Column(String)
    time = Column(BigInteger)
    side = Column(Boolean)
    type = Column(String)
    status = Column(String)
    reduce = Column(Boolean)
    price = Column(Float)
    quantity = Column(Float)



class SymbolsSettings(Base):
    __tablename__ = 'symbols_settings'
    symbol = Column(String, primary_key=True)
    status = Column(Integer, default=0)
    interval = Column(String, default='4h')
    order_size = Column(Float, default=10)
    leverage = Column(Integer, default=20)
    atr_length = Column(Integer, default=14)
    portion = Column(Float, default=0.05)
    take1 = Column(Float, default=2.5)
    take2 = Column(Float, default=5)
    stop = Column(Float, default=0.45)


class Symbols(Base):
    __tablename__ = 'symbols'
    binance_symbol = Column(String, primary_key=True, nullable=False)
    tradingview_symbol = Column(Boolean)
    trade_mode = Column(Boolean, default=False)
    last_update = Column(DateTime)

class ConfigInfo:
    api_key: str
    api_secret: str
    trade_mode: int

    def __init__(self, data):
        for key in self.__class__.__annotations__:
            setattr(self, key, None)
        for key, value in data.items():
            try:
                value = int(value)
            except:
                try:
                    value = float(value)
                except:
                    pass
            setattr(self, key, value)


# async def connect(host, port, user, password, db):
#     global Session
#     engine = create_async_engine(f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}")
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)
#     Session = async_sessionmaker(engine, expire_on_commit=False)
#     return Session



from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

async def connect(host, port, user, password, dbname):
    global Session
    DATABASE_URL = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"

    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=20,       # увеличиваем пул
        max_overflow=30,    # сколько можно создать поверх основного пула
        pool_timeout=10,    # сколько ждать свободное соединение
    )

    Session = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession
    )

    return Session




# загрузка конфигурации 
async def load_config():
    for key in ConfigInfo.__annotations__.keys():
        try:
            async with Session() as s:
                s.add(Config(key=key, value=None))
                await s.commit()
        except:
            pass

    async with Session() as s:
        result = await s.execute(select(Config))
        data = {row.key: row.value for row in result.scalars()}
        return ConfigInfo(data)

    result = await s.execute(select(Config))
    data = {row.key: row.value for row in result.scalars()}


# # функция для получения настроек для символа
# async def get_symbol_conf(symbol):
#     # создание сессии для работы с БД
#     async with Session() as s:
#         # получение настроек для символа
#         symbols = await s.execute(select(SymbolsSettings).where(SymbolsSettings.symbol == symbol))
#         # берем одну строку и возвращаем результат
#         return symbols.scalars().one_or_none()



# функция для обновления конфигурации
async def config_update(**kwargs):
    # создание сессии для работы с БД
    async with Session() as s:
        # перебираем все параметры
        for key, value in kwargs.items():
            # записываем изменения в БД
            await s.execute(update(Config).where(Config.key == key).values(value=value))
        # записываем изменения в БД
        await s.commit()


# Получаем конфигурацию по символам
async def get_symbol_conf(symbol):
    async with Session() as s:
        try:
            # Получаем настройки символа
            result = await s.execute(
                select(SymbolsSettings).where(SymbolsSettings.symbol == symbol)
            )
            settings = result.scalar_one_or_none()

            # Если настроек нет, проверяем, есть ли символ в таблице symbols и поддерживается ли TradingView
            if not settings:
                result = await s.execute(
                    select(Symbols).where(
                        Symbols.binance_symbol == symbol,
                        Symbols.tradingview_symbol.is_(True)
                    )
                )
                symbol_exists = result.scalar_one_or_none()

                if symbol_exists:
                    # Добавляем настройки по умолчанию
                    s.add(SymbolsSettings(symbol=symbol))
                    await s.commit()
                    logging.info(f"Добавлены настройки по умолчанию для {symbol}.")

                    # Повторно загружаем настройки для символа
                    result = await s.execute(
                        select(SymbolsSettings).where(SymbolsSettings.symbol == symbol)
                    )
                    settings = result.scalar_one()
                else:
                    logging.error(f"Символ {symbol} не найден в таблице symbols или не поддерживается TradingView.")
                    return None

            # Возвращаем настройки (уже гарантированно существуют)
            return settings

        except Exception as e:
            logging.error(f"Ошибка при получении данных из настройки символа: {e}")
            return None




# async def get_all_symbols_conf():
#     async with Session() as s:
#         return (await s.execute(select(SymbolsSettings))).scalars().all()

        # result = await s.execute(
        #     select(Symbols).where(Symbols.tradingview_symbol.is_(True))
        # )
        # symbols = result.scalars().all()
        # return [s.binance_symbol for s in symbols]

async def get_all_symbols_conf():
    async with Session() as s:
        try:
            # Получаем все символы, поддерживаемые TradingView
            result = await s.execute(
                select(Symbols).where(Symbols.tradingview_symbol.is_(True))
            )
            all_symbols = result.scalars().all()

            # Получаем уже существующие настройки
            result = await s.execute(select(SymbolsSettings.symbol))
            existing_settings = {row[0] for row in result.all()}

            added = 0
            for symbol_obj in all_symbols:
                if symbol_obj.binance_symbol not in existing_settings:
                    s.add(SymbolsSettings(symbol=symbol_obj.binance_symbol))
                    added += 1

            if added:
                await s.commit()
                logging.info(f"Добавлены настройки по умолчанию для {added} новых символов.")

            # Возвращаем все настройки
            result = await s.execute(select(SymbolsSettings))
            return result.scalars().all()

        except Exception as e:
            logging.error(f"Ошибка при получении или создании настроек символов: {e}")
            return []



async def save_signal_to_db(symbol: str, timeframe: str, signal: str, entry_price: float):
    async with Session() as s:
        try:
            stmt = insert(TradingviewSignals).values(
                symbol=symbol,
                interval=timeframe,
                signal=signal,
                entry_price=entry_price,
                utc_time=datetime.now(timezone.utc)
            )
            await s.execute(stmt)
            await s.commit()

        except Exception as db_e:
            logging.error(f"Ошибка при сохранении сигнала {symbol}: {db_e}")
            await s.rollback()




async def save_signals_batch_to_db(signals: list[tuple[str, str, str, float]]):
    """
    Пакетное сохранение сигналов TradingView.
    Формат signals: [(symbol, interval, signal, entry_price), ...]
    """
    async with Session() as s:
        try:
            stmt = insert(TradingviewSignals).values([
                {
                    'symbol': symbol,
                    'interval': timeframe,
                    'signal': signal,
                    'entry_price': entry_price,
                    'utc_time': datetime.now(timezone.utc)
                }
                for symbol, timeframe, signal, entry_price in signals
            ])
            await s.execute(stmt)
            await s.commit()
        except Exception as db_e:
            logging.error(f"Ошибка при батч сохранении сигналов: {db_e}")
            await s.rollback()






async def update_binance_symbols_db(symbols):
    async with Session() as s:
        try:
            for symbol in symbols:
                stmt = insert(Symbols).values(binance_symbol=symbol).on_conflict_do_nothing()
                await s.execute(stmt)
            await s.commit()
        except Exception as e:
            logging.error(f"Ошибка при вставке: {e}")
            await s.rollback()



def is_tradingview_symbols_available(symbol: str) -> bool:
    try:
        handler = TA_Handler(
            symbol=symbol,
            exchange='Binance',
            screener='crypto',
            interval=Interval.INTERVAL_1_HOUR,
        )
        handler.get_analysis()
        return True
    except Exception:
        return False




async def is_symbols_table_empty():
    async with Session() as s:
        result = await s.execute(
            select(Symbols.binance_symbol)
            .where(Symbols.tradingview_symbol.is_(True))
            .limit(1)
        )
        return result.scalar_one_or_none() is None




async def periodic_symbol_update(client, executor, lock: asyncio.Lock, hour=17, minute=30):
    while True:
        if await is_symbols_table_empty():
            logging.info("Символы с поддержкой TradingView не найдены — загружаем.")
            async with lock:
                await daily_update_symbols(client, executor)

            # Повторная проверка после обновления
            if await is_symbols_table_empty():
                logging.error("После обновления не найдено ни одного подходящего символа. Завершаем.")
                return
            else:
                logging.info("Символы успешно загружены после обновления.")
        else:
            logging.info("Символы уже есть. Продолжаем.")

        # ждём до следующего обновления
        now = datetime.now(timezone.utc)
        target_time = datetime.combine(now.date(), time(hour, minute)).replace(tzinfo=timezone.utc)
        if now >= target_time:
            target_time += timedelta(days=1)

        wait_seconds = (target_time - now).total_seconds()
        logging.info(f"Ждём {int(wait_seconds)} секунд до следующего обновления символов в {hour}:{minute} UTC...")
        await asyncio.sleep(wait_seconds)

        # 🔒 Блокируем сбор сигналов на время обновления
        async with lock:
            await daily_update_symbols(client, executor)



async def daily_update_symbols(client, executor):
    try:
        logging.info("Запуск ежедневного обновления символов...")

        # 1. Загружаем символы с Binance и обновляем БД (используем существующую функцию)
        symbols = await get_data.load_binance_symbols(client)

        # 2. Проверяем, какие из них доступны в TradingView
        binance_symbols = list(symbols.keys())
        loop = asyncio.get_running_loop()
        results = await asyncio.gather(
            *[loop.run_in_executor(executor, is_tradingview_symbols_available, symbol) for symbol in binance_symbols]
        )

        # 3. Обновляем флаги tradingview_symbol
        async with Session() as s:
            try:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                for symbol, available in zip(binance_symbols, results):
                    stmt = (
                        update(Symbols)
                        .where(Symbols.binance_symbol == symbol)
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



async def get_open_trade(symbol: str) -> Trades | None:
    async with Session() as session:
        try:
            stmt = select(Trades).where(
                Trades.symbol == symbol,
                Trades.position_open.is_(True)
            ).order_by(Trades.open_time.desc()).limit(1)

            result = await session.execute(stmt)
            return result.scalar_one_or_none()

        except Exception as e:

            print(f"Ошибка при получении открытой сделки по {symbol}: {e}")
            return None


async def sync_positions_with_exchange(client, positions: dict):
    position_info = await client.get_position_risk()

    for pos in position_info:
        symbol = pos['symbol']
        if '_' in symbol:
            continue  # Пропуск нестандартных контрактов

        amt = float(pos['positionAmt'])


        if amt != 0.0:
            positions[symbol] = True
        elif symbol in positions:
            continue
            positions[symbol] = False

# async def sync_positions_with_exchange(client, positions: dict):
#     position_info = await client.get_position_risk()
#
#     for pos in position_info:
#         symbol = pos['symbol']
#         amt = float(pos['positionAmt'])
#
#         if amt != 0.0:
#             if symbol not in positions or not positions[symbol]:
#                 # print(f"[SYNC] Найдена активная позиция на бирже: {symbol}")
#                 positions[symbol] = True
#         else:
#             if symbol not in positions:
#                 continue
#                 # positions[symbol] = False



async def get_active_entry_order_info(symbol: str, client):
    async with Session() as session:
        try:
            # Получаем открытую сделку
            trade_stmt = select(Trades).where(
                Trades.symbol == symbol,
                Trades.position_open.is_(True)
            ).order_by(Trades.open_time.desc()).limit(1)
            trade_result = await session.execute(trade_stmt)
            trade = trade_result.scalar_one_or_none()
            if not trade:
                print(f"❗ Нет открытой сделки по {symbol}")
                return None

            # Получаем входной MARKET-ордер из базы
            order_stmt = select(Orders).where(
                Orders.trade_id == trade.id,
                Orders.type == 'MARKET',
                Orders.reduce.is_(False)
            ).order_by(Orders.time.desc()).limit(1)
            order_result = await session.execute(order_stmt)
            order = order_result.scalar_one_or_none()
            if not order:
                print(f"❗ Входной ордер не найден по trade_id={trade.id} для {symbol}")
                return None

            # Получаем ПОДТВЕРЖДЁННЫЕ данные с Binance (даже если ордер FILLED)
            order_info = await client.get_all_orders(symbol=symbol, orderId=order.order_id, limit=1)
            if order_info:
                return order_info[0]
            else:
                print(f"❗ Binance не вернул данных по ордеру {order.order_id}")
                return None

        except Exception as e:
            print(f"❗ Ошибка при получении информации по ордеру {symbol}: {e}")
            return None
