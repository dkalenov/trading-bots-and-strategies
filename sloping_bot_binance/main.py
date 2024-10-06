import asyncio
import configparser
import traceback
import sloping
import db


# импортируем конфиг
config = configparser.ConfigParser()
config.read('config.ini')
# конфиг файл
conf: db.ConfigInfo


session: db.async_sessionmaker


async def main():
    global session
    global conf
    global client
    # подключаемся к базе данных
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                         config['DB']['password'], config['DB']['db'])
    # загружаем конфиг
    conf = await db.load_config()
    # создаем клиента для Binance
    client = binance.Futures(conf.api_key, conf.api_secret, asynced=True,
                             testnet=config.getboolean('BOT', 'testnet'))
    # загружаем все торговые пары
    asyncio.create_task(load_symbols())
    # подключаемся к вебсокетам
    asyncio.create_task(connect_ws())
    # запускаем бота телеграм
    await tg.run(session, client, connect_ws, disconnect_ws, subscribe_ws, unsubscribe_ws, remove_indicator)


if __name__ == '__main__':
    # загружаем конфиг
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())
