import configparser
import binance
import db
import asyncio


# функция запуска бота
async def main():
    # глобальные переменные
    global client
    global all_symbols
    global conf
    global session
    # подключаемся к базе данных
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                               config['DB']['password'], config['DB']['db'])
    # загружаем конфиг
    conf = await db.load_config()
