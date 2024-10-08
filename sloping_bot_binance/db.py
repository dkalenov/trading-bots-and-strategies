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
