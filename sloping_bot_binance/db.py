from sqlalchemy import Column, Integer, BigInteger, String, Float, Boolean, select, delete, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


# создаем базовый класс для работы с БД
Base = declarative_base()
Session: async_sessionmaker


# создаем таблицу с основными настройками
class Config(Base):
    __tablename__ = 'config'
    key = Column(String, primary_key=True)
    value = Column(String)


# создаем таблицу с настройками символов
class SymbolsSettings(Base):
    __tablename__ = 'symbols_settings'
    symbol = Column(String, primary_key=True)
    status = Column(Integer, default=0)
    interval = Column(String, default='1m')
    order_size = Column(Float, default=10)
    leverage = Column(Integer, default=20)
    length = Column(Integer, default=50)
    atr_length = Column(Integer, default=14)
    take = Column(Float, default=1)
    stop = Column(Float, default=1)


# создаем таблицу для хранения сделок
class Trades(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    order_size = Column(Float)
    side = Column(Boolean)
    status = Column(String)
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    entry_price = Column(Float)
    quantity = Column(Float)
    take_price = Column(Float)
    stop_price = Column(Float)
    result = Column(Float, default=0)
    msg_id = Column(Integer)


# создаем таблицу для хранения ордеров
class Orders(Base):
    __tablename__ = 'orders'
    order_id = Column(BigInteger, primary_key=True)
    trade_id = Column(Integer)
    symbol = Column(String)
    time = Column(BigInteger)
    side = Column(Boolean)
    type = Column(String)
    status = Column(String)
    reduce = Column(Boolean)
    price = Column(Float)
    quantity = Column(Float)

# класс для хранения конфигации
class ConfigInfo:
    api_key: str
    api_secret: str
    trade_mode: int

    def __init__(self, data):
        for key in self.__class__.__annotations__:
            setattr(self, key, None)
        # преобразование данных
        for key, value in data.items():
            try:
                # пытаемся преобразовать в int
                value = int(value)
            except:
                # если не получилось
                try:
                    # пытаемся преобразовать в float
                    value = float(value)
                except:
                    # если не получилось, то пропускаем и он будет строкой
                    pass
            # присваиваем значение значению класса
            setattr(self, key, value)

async def connect(host, port, user, password, db_name):
    # глобальная переменная сессии
    global Session
    # создаем асинхронный движок для работы с БД используя конфигурацию из config.py
    engine = create_async_engine(f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}")
    async with engine.begin() as conn:
        # создаем таблицы
        await conn.run_sync(Base.metadata.create_all)
    # создаем сессию, expire_on_commit=False - чтобы она не уничтожалась после коммита
    Session = async_sessionmaker(engine, expire_on_commit=False)
    # возвращаем сессию
    return Session


async def load_config():
    # создаем пустые поля в БД
    for key in ConfigInfo.__annotations__.keys():
        try:
            async with Session() as s:
                s.add(Config(key=key, value=None))
                await s.commit()
        except:
            pass
    # создание сессии для работы с БД
    async with Session() as session:
        # загрузка конфигурации
        result = (await session.execute(select(Config))).scalars().all()
        # преобразование данных
        data = {row.key: row.value for row in result}
        # возвращаем результат
        return ConfigInfo(data)


# функция для получения настроек для символа
async def get_symbol_conf(symbol):
    # создание сессии для работы с БД
    async with Session() as s:
        # получение настроек для символа
        symbols = await s.execute(select(SymbolsSettings).where(SymbolsSettings.symbol == symbol))
        # берем одну строку и возвращаем результат
        return symbols.scalars().one()
