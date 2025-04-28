import asyncio
import db
import configparser

conf: db.ConfigInfo


async def main():
    global session
    global conf
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                         config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()
    print(conf.__dict__)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    asyncio.run(main())
