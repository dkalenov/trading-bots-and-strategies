from sqlalchemy import BigInteger, Boolean, Column, Float, Integer, String, delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
Session: async_sessionmaker


class Config(Base):
    __tablename__ = "config"

    key = Column(String, primary_key=True)
    value = Column(String)


class SymbolsSettings(Base):
    __tablename__ = "symbols_settings"

    symbol = Column(String, primary_key=True)
    status = Column(Integer, default=0)
    interval = Column(String, default="1m")
    order_size = Column(Float, default=10)
    leverage = Column(Integer, default=20)
    length = Column(Integer, default=50)
    atr_length = Column(Integer, default=14)
    take1 = Column(Float, default=1.0)
    take2 = Column(Float, default=2.5)
    stop = Column(Float, default=1.0)
    portion = Column(Float, default=0.5)
    risk_pct = Column(Float, default=0.01)
    max_positions = Column(Integer, default=5)
    # --- Параметры фильтров Sloping ---
    min_space = Column(Integer, default=3)
    min_touches = Column(Integer, default=2)
    breakout_buffer = Column(Float, default=0.1)
    slope_filter = Column(Boolean, default=True)
    use_trend_filter = Column(Boolean, default=True)
    trend_sma = Column(Integer, default=200)
    vol_filter = Column(Boolean, default=False)
    vol_lookback = Column(Integer, default=5)
    trail_after_tp1 = Column(Boolean, default=False)
    trail_atr = Column(Float, default=1.5)


class Trades(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    order_size = Column(Float)
    side = Column(Boolean)
    status = Column(String)
    position_open = Column(Boolean, default=True)
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    interval = Column(String)
    window_size = Column(Integer)
    leverage = Column(Integer)
    entry_price = Column(Float)
    quantity = Column(Float)
    atr_length = Column(Integer)
    atr = Column(Float)
    take1_atr = Column(Float)
    take2_atr = Column(Float)
    loss_atr = Column(Float)
    take1_price = Column(Float)
    take2_price = Column(Float)
    stop_price = Column(Float)
    breakeven_stop_price = Column(Float)
    take1_triggered = Column(Boolean, default=False)
    partial_exit_done = Column(Boolean, default=False)
    trail_high = Column(Float)
    result = Column(Float, default=0)
    msg_id = Column(Integer)


class Orders(Base):
    __tablename__ = "orders"

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
    realized_profit = Column(Float, default=0.0)


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
            except Exception:
                try:
                    value = float(value)
                except Exception:
                    pass
            setattr(self, key, value)


async def connect(host, port, user, password, db_name):
    global Session

    engine = create_async_engine(f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}")
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
        except Exception:
            pass

    async with Session() as session:
        result = (await session.execute(select(Config))).scalars().all()
        data = {row.key: row.value for row in result}
        return ConfigInfo(data)


async def get_symbol_conf(symbol):
    async with Session() as s:
        symbols = await s.execute(select(SymbolsSettings).where(SymbolsSettings.symbol == symbol))
        return symbols.scalars().one()


async def get_all_symbols():
    async with Session() as s:
        symbols = await s.execute(select(SymbolsSettings))
        return symbols.scalars().all()


async def symbol_update(symbol):
    async with Session() as s:
        s.add(symbol)
        await s.commit()


async def symbol_add(symbol):
    async with Session() as s:
        s.add(SymbolsSettings(symbol=symbol))
        await s.commit()


async def symbol_delete(symbol):
    async with Session() as s:
        await s.execute(delete(SymbolsSettings).where(SymbolsSettings.symbol == symbol))
        await s.commit()


async def get_open_trades():
    async with Session() as s:
        trades = await s.execute(select(Trades).where(Trades.position_open.is_(True)))
        return trades.scalars().all()


async def config_update(**kwargs):
    async with Session() as s:
        for key, value in kwargs.items():
            await s.execute(update(Config).where(Config.key == key).values(value=value))
        await s.commit()


async def get_order_trade(order_id):
    async with Session() as s:
        order = (await s.execute(select(Orders).where(Orders.order_id == order_id))).scalar_one_or_none()
        if order:
            trade = (await s.execute(select(Trades).where(Trades.id == order.trade_id))).scalar_one_or_none()
        else:
            trade = None
        return order, trade


async def update_order_trade(order, trade):
    async with Session() as s:
        await s.merge(order)
        await s.merge(trade)
        await s.commit()


async def get_last_trade(symbol):
    async with Session() as s:
        trade = await s.execute(
            select(Trades).where(Trades.symbol == symbol).order_by(Trades.open_time.desc()).limit(1)
        )
        return trade.scalar_one_or_none()


async def get_trade_orders(trade_id):
    async with Session() as s:
        orders = await s.execute(select(Orders).where(Orders.trade_id == trade_id))
        return orders.scalars().all()


async def get_open_trade(symbol):
    """Получить текущую открытую сделку по символу."""
    async with Session() as s:
        result = await s.execute(
            select(Trades).where(
                Trades.symbol == symbol,
                Trades.position_open.is_(True),
            ).order_by(Trades.open_time.desc()).limit(1)
        )
        return result.scalar_one_or_none()


async def get_last_active_stop_order(trade_id):
    """Получить последний активный STOP_MARKET ордер для сделки."""
    async with Session() as s:
        result = await s.execute(
            select(Orders).where(
                Orders.trade_id == trade_id,
                Orders.type == "STOP_MARKET",
                Orders.reduce.is_(True),
                Orders.status == "NEW",
            ).order_by(Orders.time.desc())
        )
        return result.scalar_one_or_none()


async def update_trade_result(trade_id):
    """Пересчитать result сделки суммой realized_profit всех ордеров."""
    async with Session() as s:
        result = await s.execute(
            select(func.sum(Orders.realized_profit)).where(Orders.trade_id == trade_id)
        )
        total_profit = result.scalar() or 0.0
        await s.execute(
            update(Trades).where(Trades.id == trade_id).values(result=total_profit)
        )
        await s.commit()


async def get_open_positions_count():
    """Получить количество открытых позиций."""
    async with Session() as s:
        result = await s.execute(
            select(func.count()).select_from(Trades).where(Trades.position_open.is_(True))
        )
        return result.scalar() or 0
