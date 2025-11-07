import configparser
import binance
import db
import asyncio



async def main():
    # глобальные переменные
    global client
    global all_symbols
    global conf
    global session

    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                               config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()


@dataclass
class LevelSnapshot:
    """
    Represents a bucketed price level (aggregated) with statistics.
    """
    price: float                      # canonical price of the bucket
    side: str                         # 'bid' or 'ask'
    volume: float                     # current aggregated volume at this price
    last_update: float = field(default_factory=time.time)
    first_seen: float = field(default_factory=time.time)
    history: deque = field(default_factory=lambda: deque(maxlen=256))  # (timestamp, volume) pairs

    def update(self, volume: float, ts: Optional[float] = None):
        ts = ts if ts is not None else time.time()
        self.volume = volume
        self.last_update = ts
        self.history.append((ts, volume))
