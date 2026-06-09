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


# создаем таблицу с трейдами
class Trades(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    order_size = Column(Float)
    status = Column(String)
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    entry_price = Column(Float)
    quantity = Column(Float)
    take_price = Column(Float)
    stop_price = Column(Float)
    profit = Column(Float)
    tg_message_id = Column(Integer)


# класс для хранения конфигации
class ConfigInfo:
    api_key: str = ''
    api_secret: str = ''
    trade_mode: int = 0
    dry_run: int = 0
    depth: int = 20
    streams_count: int = 50
    ask_volume: float = 5.0
    order_size: float = 10.0
    max_positions: int = 5
    min_volume: float = 5000.0
    min_volume_24h: float = 1_000_000.0
    max_volume_24h: float = 1_000_000_000.0
    min_density_num: int = 7
    entry_timeout: int = 30
    take_profit: float = 0.5
    blacklist: str = 'BTCUSDT,ETHUSDT'
    stop_loss_ticks: int = 3
    leverage: int = 20
    enable_instant_exits: int = 1
    enable_be: int = 1
    be_trigger_pct: float = 0.25
    be_offset_ticks: int = 1
    score_threshold: int = 12


    def __init__(self, data):
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
            if key == 'blacklist':
                # присваиваем значение значению класса
                value = value.split(',')
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


# функция для загрузки конфигурации
async def load_config():
    # создаем пустые поля в БД
    config_info = ConfigInfo({})
    for key in config_info.__dir__():
        if not key.startswith('_'):
            try:
                async with Session() as s:
                    s.add(Config(key=key, value=str(getattr(config_info, key))))
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


# функция для обновления конфигурации
async def config_update(**kwargs):
    # создание сессии для работы с БД
    async with Session() as s:
        # перебираем все параметры
        for key, value in kwargs.items():
            # записываем изменения в БД
            await s.execute(update(Config).where(Config.key == key).values(value=str(value)))
        # записываем изменения в БД
        await s.commit()


# функция для получения сделки
async def get_trade(symbol):
    # создание сессии для работы с БД
    async with Session() as s:
        # возвращаем результат
        return (await s.execute(select(Trades).where(Trades.symbol == symbol).where(Trades.status == 'OPEN').
                                  order_by(Trades.open_time.desc()).limit(1))).scalar_one_or_none()


# функция для обновления ордера и сделки
async def update_trade(trade):
    # создание сессии для работы с БД
    async with Session() as s:
        # обновляем трейд
        s.add(trade)
        # записываем изменения в БД
        await s.commit()


# функция для получения всех сделок
async def get_all_trades():
    # создание сессии для работы с БД
    async with Session() as s:
        # возвращаем результат
        return (await s.execute(select(Trades).where(Trades.status == 'OPEN'))).scalars().all()


# функция для получения сделок с определенной даты
async def get_trades_ts(timestamp):
    # создание сессии для работы с БД
    async with Session() as s:
        # возвращаем результат
        return (await s.execute(select(Trades).where(Trades.open_time >= timestamp))).scalars().all()
