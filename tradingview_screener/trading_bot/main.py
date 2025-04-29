import asyncio
from sys import excepthook

import db
import configparser
import binance

conf: db.ConfigInfo
client: binance.Futures
all_symbols: dict[str, binance.SymbolFutures] = {}


async def main():
    global session
    global conf
    global client
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                         config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()
    client = binance.Futures(conf.api_key, conf.api_secret, asynced=True,
                             testnet=config.getboolean('BOT', 'testnet'))

    asyncio.create_task(load_symbols())


async def load_symbols():
    global all_symbols
    while True:
        try:
            all_symbols = await client.load_symbols()
        except:
            pass
        finally:
            await asyncio.sleep(3600)




if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())
