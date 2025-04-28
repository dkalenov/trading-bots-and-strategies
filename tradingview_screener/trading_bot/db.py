from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, Float, Boolean, DateTime, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

Base = declarative_base()
Session: async_sessionmaker


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
    order_size = Column(Float)
    side = Column(Boolean)
    status = Column(String)
    open_time = BigInteger()
    close_time = BigInteger()
    entry_price = Column(Float)
    quantity = Column(Float)
    take1_price = Column(Float)
    take2_price = Column(Float)
    stop_price = Column(Float)
    breakeven_stop_price = Column(Float)
    result = Column(Float)
    msg_id = Column(Integer)


class Orders(Base):
    __tablename__ = 'orders'
    order_id = Column(Integer, primary_key=True, autoincrement=True)
    trade_id = Column(Integer)
    symbol = Column(String)
    time = Column(BigInteger)
    side = Column(Boolean)
    type = Column(String)
    status = Column(String)
    reduce = Column(Boolean)
    price = Column(Float)
    quantity = Column(Float)

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


async def connect(host, port, user, password, db):
    global Session
    engine = create_async_engine(f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    return Session

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