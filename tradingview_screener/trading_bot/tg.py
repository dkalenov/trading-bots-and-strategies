import asyncio
import configparser
import binance
import db
import os
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton)


# создаем бота и диспетчер
bot: Bot
dp = Dispatcher()
# загружаем конфиг
config = configparser.ConfigParser()
config.read('config.ini')
# парсим список админов
tg_admins = list(map(int, config['TG']['admins'].split(',')))



# фунция для запуска бота
async def run():
    # передаем функции и параметры из файла main
    global bot
    global dp

    bot = Bot(token=config['TG']['token'], default=DefaultBotProperties(parse_mode='HTML'))
    # удаляем старые сообщения
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        # запускаем бота
        await dp.start_polling(bot)
    finally:
        # закрываем сессию
        await bot.session.close()