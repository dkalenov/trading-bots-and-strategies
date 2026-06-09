import asyncio
import configparser
import datetime
import binance
import db
import os
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
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
tg_admins = list(map(int, config['TG']['ADMINS'].split(',')))
# глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
tg_queue: asyncio.Queue


# состояния бота
class States(StatesGroup):
    main_menu = State()
    settings = State()
    trade_mode = State()
    change_keys = State()
    edit_settings = State()
    blacklist_add = State()
    restart = State()
    statistics = State()
    positions = State()
    close_pos = State()


# описание настроек в боте
settings_name_text = {
    'depth': 'глубину стакана',
    'streams_count': 'кол-во стримов на вебсокет',
    'ask_volume': 'множитель объема плотности',
    'order_size': 'размер ордера',
    'max_positions': 'макс. кол-во позиций',
    'min_volume': 'мин. объем плотности',
    'min_volume_24h': 'мин. объем плотности за 24 часа',
    'max_volume_24h': 'макс. объем плотности за 24 часа',
    'min_density_num': 'расстояние до плотности',
    'entry_timeout': 'тайм-аут отмены лимитки',
    'take_profit': 'размер тейка',
    'blacklist': 'черный список',
    'stop_loss_ticks': 'размер стопа в тиках',
    'leverage': 'размер плеча',
}
# варианты глубины стакана на бирже
depths = [5, 10, 20]
# параметры, при изменении которых необходимо перезапускать вебсокеты
settings_reconnect_ws = ['depth', 'streams_count', 'ask_volume']

# функция для запуска бота
async def run(_conf, _client, _tg_queue):
    # передаем функции и параметры из файла main
    global bot
    global dp
    global conf
    global client
    global tg_queue
    conf = _conf
    client = _client
    tg_queue = _tg_queue
    # инициализируем бота
    bot = Bot(token=config['TG']['TOKEN'], default=DefaultBotProperties(parse_mode='HTML'))
    # удаляем старые сообщения
    await bot.delete_webhook(drop_pending_updates=True)
    
    # отправляем уведомление о запуске бота в Telegram-канал
    try:
        startup_text = "Density Bot запущен"
        await bot.send_message(config.getint('TG', 'CHANNEL'), startup_text)
        print("[Telegram] Отправлено уведомление о запуске бота в канал.")
    except Exception as e:
        print(f"[Telegram] Не удалось отправить уведомление о запуске: {e}")

    try:
        # запускаем бота
        await dp.start_polling(bot)
    finally:
        # закрываем сессию
        await bot.session.close()


# функция для перезагрузки конфига
async def reload_config():
    global conf
    # перезагружаем конфиг
    conf = await db.load_config()
    # отправляем команду в основной модуль
    await tg_queue.put('reload_config')


# функция для переподключения вебсокетов
async def reconnect_ws():
    # отправляем команду в основной модуль
    await tg_queue.put('reconnect_ws')


# функция для закрытия позиций
async def close_position(symbol=None):
    # отправляем команду в основной модуль
    await tg_queue.put(('close_position', symbol))
    # ждем 5 секунд
    await asyncio.sleep(5)


# функция для загрузки открытых позиций
async def get_positions():
    # Если запущен симуляционный режим (dry run), просто возвращаем все сделки со статусом OPEN из БД
    if conf.dry_run:
        return await db.get_all_trades()

    # формируем список монет
    coins = {}
    try:
        account_info = await client.account()
        # проверяем, является ли это Futures аккаунтом или Spot
        if 'positions' in account_info:
            for pos in account_info.get('positions', []):
                amt = float(pos.get('positionAmt', 0))
                if amt != 0:
                    coins[pos['symbol']] = abs(amt)
        elif 'balances' in account_info:
            for coin in account_info['balances']:
                if value := float(coin['free']) + float(coin['locked']):
                    coins[coin['asset']] = value
    except Exception as e:
        print(f"Error fetching account info: {e}")

    # формируем список с открытыми позициями
    trades = []
    # загружаем сделки из базы данных и перебираем их в цикле
    for trade in (await db.get_all_trades()):
        # Если это фьючерсы, проверяем наличие пары в coins
        if trade.symbol in coins:
            trades.append(trade)
        else:
            # Для совместимости со старой логикой Spot
            for coin in coins:
                if f"{coin}{config['BOT']['ASSET']}" == trade.symbol and trade.quantity <= coins[coin]:
                    trades.append(trade)
                    break
    # возвращаем результат
    return trades


# пропускаем сообщения не от администраторов
@dp.message(~F.from_user.id.in_(tg_admins))
async def skip(msg):
    pass


# функция вывода главного меню
# Command("start", "menu") - реагирует на /start и /menu
# F.text == "Главное меню" - реагирует на текст "Главное меню" (от кнопки в меню)
@dp.message(Command("start", "menu"))
@dp.message(F.text == "Главное меню")
async def start(message: Message, state: FSMContext):
    # устанавливаем состояние главного меню
    await state.set_state(States.main_menu)
    # создаем клавиатуру главного меню
    # resize_keyboard=True - уменьшает высоту клавиатуры
    keyboard = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Открытые позиции"), KeyboardButton(text="Статистика")],
        [KeyboardButton(text="Настройки"), KeyboardButton(text="Главное меню")]
    ], resize_keyboard=True)
    # отправляем сообщение с клавиатурой
    await message.answer("Главное меню", reply_markup=keyboard)


# функция вывода меню настроек
@dp.message(F.text == "Настройки")
@dp.callback_query(F.data == "settings")
async def settings(message: Message, state: FSMContext):
    # устанавливаем состояние настроек
    await state.set_state(States.settings)
    # создаем кнопки для включения/отключения торговли и изменения ключей
    keyboard = [
        [InlineKeyboardButton(text=f"{'Отключить' if conf.trade_mode else 'Включить'} торговлю",
                              callback_data="trade_mode")],
        [InlineKeyboardButton(text=f"Режим: {'Симуляция (Dry Run)' if conf.dry_run else 'Реальный'}",
                              callback_data="toggle_dry_run")],
        [InlineKeyboardButton(text="Изменить ключи", callback_data="change_keys")],
    ]
    # генерируем кнопки для настроек
    for key, values in settings_name_text.items():
        keyboard.append([InlineKeyboardButton(text=f"Изменить {values}", callback_data=f"settings_edit:{key}")])
    # добавляем кнопку перезагрузки бота
    keyboard.append([InlineKeyboardButton(text="Перезагрузка бота", callback_data="restart")])
    # создаем текст API ключа
    api_key = f"{conf.api_key[:4]}...{conf.api_key[-4:]}" if conf.api_key else "Отсутствует"
    # создаем текст для черного списка
    blacklist = ", ".join(conf.blacklist) if conf.blacklist and conf.blacklist[0] else "Отсутствует"
    # формируем текст сообщения
    text = (f"Основные настройки:\n"
            f"Торговля: <b>{'Включена' if conf.trade_mode else 'Отключена'}</b>\n"
            f"Режим выполнения: <b>{'Симуляция (Dry Run)' if conf.dry_run else 'Реальная торговля'}</b>\n"
            f"API KEY: <b>{api_key}</b>\n"
            f"Глубина стакана: <b>{conf.depth}</b>\n"
            f"Множитель объема плотности: <b>{conf.ask_volume}</b>\n"
            f"Кол-во стримов на вебсокет: <b>{conf.streams_count}</b>\n"
            f"Размер ордера: <b>{conf.order_size} {config['BOT']['ASSET']}</b>\n"
            f"Размер плеча: <b>{conf.leverage} x</b>\n"
            f"Макс. кол-во позиций: <b>{conf.max_positions}</b>\n"
            f"Мин. объем плотности: <b>{conf.min_volume} {config['BOT']['ASSET']}</b>\n"
            f"Мин. объем плотности за 24 часа: <b>{conf.min_volume_24h} {config['BOT']['ASSET']}</b>\n"
            f"Макс. объем плотности за 24 часа: <b>{conf.max_volume_24h} {config['BOT']['ASSET']}</b>\n"
            f"Расстояние до плотности: <b>{conf.min_density_num}</b>\n"
            f"Тайм-аут отмены лимитки: <b>{conf.entry_timeout} сек.</b>\n"
            f"Размер тейка: <b>{conf.take_profit} %</b>\n"
            f"Размер стопа: <b>{conf.stop_loss_ticks} тиков</b>\n"
            f"Черный список: <b>{blacklist}</b>\n")
    # если нас вызвал callback
    if isinstance(message, CallbackQuery):
        # отвечаем на callback
        await message.answer()
        # выбираем сообщение из callback'а
        message = message.message
    # отправляем сообщение
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# функция включения или выключения торговли
@dp.callback_query(States.settings, F.data == "trade_mode")
async def trade_mode(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для подтверждения перезагрузки
    await state.set_state(States.trade_mode)
    # формируем клавиатуру с подтверждением удаления
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="trade_mode_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="settings")]
    ])
    # отправляем callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer(f"Вы действительно хотите {'отключить' if conf.trade_mode else 'включить'} "
                                  f"торговлю?", reply_markup=keyboard)


# функция подтверждения включения или выключения торговли
@dp.callback_query(States.trade_mode, F.data == "trade_mode_yes")
async def trade_mode_yes(callback: CallbackQuery, state: FSMContext):
    # обновляем настройки в базе
    await db.config_update(trade_mode='0' if conf.trade_mode else '1')
    await reload_config()
    # отправляем сообщение
    await callback.answer(f"Торговля успешно {'включена' if conf.trade_mode else 'отключена'}")
    # закрываем позиции
    await close_position()
    # перезагружаем настройки
    await reconnect_ws()
    # вызываем меню настроек
    await settings(callback.message, state)


# функция переключения режима симуляции (dry run)
@dp.callback_query(States.settings, F.data == "toggle_dry_run")
async def toggle_dry_run(callback: CallbackQuery, state: FSMContext):
    # обновляем настройки в базе
    new_dry_run = '0' if conf.dry_run else '1'
    await db.config_update(dry_run=new_dry_run)
    await reload_config()
    # отправляем сообщение
    mode_text = "Симуляция (Dry Run)" if new_dry_run == '1' else "Реальная торговля"
    await callback.answer(f"Переключено на режим: {mode_text}")
    # переподключаем вебсокеты, чтобы применить новый режим
    await reconnect_ws()
    # вызываем меню настроек
    await settings(callback.message, state)


# функция для изменения ключей
@dp.callback_query(States.settings, F.data == "change_keys")
async def change_keys(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для изменения ключей
    await state.set_state(States.change_keys)
    # отправляем callback
    await callback.answer()
    # запрашиваем API KEY
    await callback.message.answer("Введите API KEY:")


# функция для изменения ключей
@dp.message(States.change_keys)
async def change_keys_value(message: Message, state: FSMContext):
    global config
    # получаем состояние
    data = await state.get_data()
    # если API KEY еще не был введен
    if not data.get('api_key'):
        # записываем API KEY
        await state.update_data(api_key=message.text.strip())
        # удаляем сообщение с API-ключом для безопасности
        try:
            await message.delete()
        except Exception as e:
            print(f"Не удалось удалить сообщение с API KEY: {e}")
        # запрашиваем SECRET KEY
        await message.answer("Введите SECRET KEY:")
    else:
        # берем API KEY и SECRET KEY
        api_key, secret_key = data['api_key'], message.text.strip()
        # удаляем сообщение с SECRET-ключом для безопасности
        try:
            await message.delete()
        except Exception as e:
            print(f"Не удалось удалить сообщение с SECRET KEY: {e}")
        # пробуем подключиться с ними к бирже
        tmp_client = binance.Futures(api_key=api_key, secret_key=secret_key, asynced=True,
                                     testnet=config.getboolean('BOT', 'TESTNET'))
        try:
            # и проверяем их корректность
            await tmp_client.account()
        except Exception as e:
            print(f"[API KEY ERROR] Validation failed: {e}")
            import traceback
            traceback.print_exc()
            # в случае ошибки удаляем API KEY
            await state.update_data(api_key=None)
            # и запрашиваем его заново
            await message.answer("Ключи некорректны, попробуйте еще раз\n"
                                 "Введите API KEY:")
            return
        finally:
            # в любом случае удаляем временный клиент
            await tmp_client.close()
        # обновляем ключи в базе
        await db.config_update(api_key=api_key, secret_key=secret_key)
        # отправляем сообщение
        await message.answer("Ключи успешно обновлены, перезагружаю бота")
        # перезапускаем бота
        await restart_yes(message, state)


# функция для изменения черного списка
@dp.callback_query(States.settings, F.data == "settings_edit:blacklist")
async def edit_blacklist(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние
    await state.set_state(States.settings)
    keyboard = [
        [InlineKeyboardButton(text="Добавить пару в черный список", callback_data="blacklist_add")]
    ]
    # если черный список не пуст
    if conf.blacklist and conf.blacklist[0]:
        # добавляем в клавиатуру кнопку удаления
        keyboard.append([InlineKeyboardButton(text="Удалить пару из черного списка", callback_data="blacklist_delete")])
        # формируем текст сообщения
        text = (f"Вы хотите изменить черный список торговых пар?\n"
                f"Торговые пары в черном списке: <b>{', '.join(conf.blacklist)}</b>\n")
    else:
        # формируем текст сообщения
        text = ("Вы хотите изменить черный список торговых пар?\n"
                "Черный список пуст")
    # отправляем callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# функция для добавления в черный список
@dp.callback_query(States.settings, F.data == "blacklist_add")
async def blacklist_add(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние добавления в черный список
    await state.set_state(States.blacklist_add)
    # отправляем callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer("Введите пару для добавления в черный список:")


# функция для добавления в черный список
@dp.message(States.blacklist_add)
async def blacklist_add_value(message: Message, state: FSMContext):
    # получаем торговую пару из сообщения и переводим ее в верхний регистр
    symbol = message.text.strip().upper()
    # проверяем наличие такой пары на бирже
    if symbol not in (await client.load_symbols()):
        # если такая пара не существует, то отправляем сообщение об ошибке
        await message.answer("Такой пары не существует, попробуйте еще раз")
        # и выходим из функции
        return
    # добавляем пару в черный список
    conf.blacklist.append(symbol)
    # обновляем черный список в базе
    await db.config_update(blacklist=','.join([s for s in conf.blacklist if s]))
    # перезагружаем настройки
    await reload_config()
    # отправляем сообщение
    await message.answer(f"Пара {symbol} успешно добавлена в черный список")
    # выводим меню настроек
    await settings(message, state)


# функция для удаления из черного списка
@dp.callback_query(States.settings, F.data == "blacklist_delete")
async def blacklist_delete(callback: CallbackQuery, state: FSMContext):
    # создаем клавиатуру
    keyboard = []
    # перебираем все элементы черного списка
    for sym in conf.blacklist:
        if sym:
            # добавляем в клавиатуру кнопки с элементами черного списка
            keyboard.append([InlineKeyboardButton(text=sym, callback_data=f"blacklist_delete:{sym}")])
    # отправляем callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer("Выберите пару, которую хотите удалить из черного списка:",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# функция для удаления из черного списка
@dp.callback_query(States.settings, F.data.startswith("blacklist_delete:"))
async def blacklist_delete(callback: CallbackQuery, state: FSMContext):
    # получаем торговую пару из callback'а
    symbol = callback.data.split(':')[-1]
    # удаляем пару из черного списка
    conf.blacklist.remove(symbol)
    # обновляем черный список в базе
    await db.config_update(blacklist=','.join([s for s in conf.blacklist if s]))
    # перезагружаем настройки
    await reload_config()
    # отправляем сообщение
    await callback.answer(f"Пара {symbol} успешно удалена из черного списка")
    # выводим меню настроек
    await settings(callback.message, state)


# функция для изменения настроек (универсальная)
@dp.callback_query(States.settings, F.data.startswith("settings_edit:"))
async def settings_edit(callback: CallbackQuery, state: FSMContext):
    # получаем имя параметра из callback'а
    edit_name = callback.data.split(':')[-1]
    # устанавливаем состояние изменения настроек
    await state.set_state(States.edit_settings)
    # сохраняем имя параметра в состоянии
    await state.update_data(edit_name=edit_name)
    # отправляем callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer(f"Вы хотите изменить {settings_name_text[edit_name]}?\n"
                                  f"Текущее значение: <b>{getattr(conf, edit_name)}</b>\n"
                                  f"Введите новое значение:")


# функция для изменения настроек
@dp.message(States.edit_settings)
async def settings_edit_value(message: Message, state: FSMContext):
    # получаем данные из состояния
    data = await state.get_data()
    # получаем имя параметра, которое мы запомнили
    edit_name = data['edit_name']
    # проверяем наличие такого параметра в конфигурации
    if (attr := getattr(conf, edit_name)) is not None:
        try:
            # пытаемся преобразовать новое значение в нужный тип
            value = type(attr)(message.text.strip())
            # если новое значение является числом, то провряем чтобы оно было больше нуля
            if type(value) in (int, float) and value <= 0:
                raise ValueError
        except:
            # в случае ошибки выводим сообщение
            await message.answer("Некорректное значение, попробуйте еще раз")
            # и выходим из функции
            return
        # делаем проверки для необходимых параметров
        match edit_name:
            # проверяем глубину стакана
            case 'depth':
                # если глубина не входит в список допустимых значений
                if value not in depths:
                    # выводим сообщение
                    await message.answer(f"Некорректное значение глубины сткана, допустимые значения: "
                                         f"{','.join(map(str, depths))}")
                    # и выходим из функции
                    return
        # обновляем настройки в базе
        await db.config_update(**{edit_name: value})
        # перезагружаем настройки
        await reload_config()
        if edit_name in settings_reconnect_ws:
            # если параметр требует перезагрузки вебсокетов
            await reconnect_ws()
        # отправляем сообщение
        await message.answer(f"Новое значение {settings_name_text[edit_name]} установленно: "
                             f"<b>{getattr(conf, edit_name)}</b>")
        # выводим меню настроек
        await settings(message, state)


# функция для перезагрузки бота
@dp.callback_query(States.settings, F.data == "restart")
async def restart(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для подтверждения перезагрузки
    await state.set_state(States.restart)
    # создаем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="restart_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="settings")]
    ])
    # отправляем callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer("Вы действительно хотите перезапустить бота?", reply_markup=keyboard)


# функция для подтверждения перезагрузки
@dp.callback_query(States.restart, F.data == "restart_yes")
async def restart_yes(callback: CallbackQuery, state: FSMContext):
    try:
        # закрываем все позиции
        await close_position()
        if isinstance(callback, CallbackQuery):
            # выводим сообщение
            await callback.answer("Перезагрузка...")
    finally:
        # перезагружаем бота
        os._exit(0)


# функция для вывода статистики
@dp.message(F.text == "Статистика")
@dp.callback_query(F.data.startswith("statistics:"))
async def statistics(message: Message, state: FSMContext):
    # устанавливаем состояние для вывода статистики
    await state.set_state(States.statistics)
    # получаем параметр периода вывода статистики
    match message.data.split(':')[1] if isinstance(message, CallbackQuery) else '24h':
        # выбираем дельту для периода
        case '24h':
            period = "24 часа"
            delta = datetime.timedelta(days=1)
        case '7d':
            period = "7 дней"
            delta = datetime.timedelta(days=7)
        case '30d':
            period = "30 дней"
            delta = datetime.timedelta(days=30)
        case _:
            period = "все время"
            delta = None
    # вычисляем начало периода вывода статистики
    ts_delta = int((datetime.datetime.now() - delta).timestamp() * 1000) if delta else 0
    # инициализируем счетчики
    sum_profit = 0
    count_trades = 0
    count_open_trades = 0
    count_profit = 0
    count_stop_loss = 0
    # перебираем все сделки в заданный период
    for trade in (await db.get_trades_ts(ts_delta)):
        # вычисляем прибыль
        profit = trade.profit or 0
        # добавляем прибыль к общей сумме
        sum_profit += profit
        # увеличиваем счетчик количества сделок
        count_trades += 1
        # если прибыль больше нуля
        if profit > 0:
            # увеличиваем счетчик прибыльных сделок
            count_profit += 1
        # если прибыль меньше нуля
        else:
            # увеличиваем счетчик убыточных сделок
            count_stop_loss += 1
        # если сделка открыта
        if trade.status == 'OPEN':
            # увеличиваем счетчик открытых сделок
            count_open_trades += 1
    # формируем текст статистики
    text = (f"Статистика за {period}:\n"
            f"Всего сделок: <b>{count_trades}</b>\n"
            f"Открытых позиций: <b>{count_open_trades}</b>\n"
            f"Прибыльных сделок: <b>{count_profit}</b>\n"
            f"Убыточных сделок: <b>{count_stop_loss}</b>\n"
            f"Реализованн{'ая прибыль' if sum_profit > 0 else 'ый убыток'}: <b>{sum_profit:.2f} "
            f"{config['BOT']['ASSET']}</b>")
    # создаем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="За 24Ч", callback_data="statistics:24h"),
         InlineKeyboardButton(text="За 7Д", callback_data="statistics:7d"),],
        [InlineKeyboardButton(text="За 30Д", callback_data="statistics:30d"),
         InlineKeyboardButton(text="За все время", callback_data="statistics:all"), ]
    ])
    # если вызов был из callback
    if isinstance(message, CallbackQuery):
        # отвечаем на callback
        await message.answer()
        # получаем message
        message = message.message
    # отправляем сообщение
    await message.answer(text, reply_markup=keyboard)


# функция для вывода открытых позиций
@dp.message(F.text == "Открытые позиции")
@dp.callback_query(F.data == "positions")
async def open_positions(message: Message, state: FSMContext):
    # устанавливаем состояние для вывода открытых позиций
    await state.set_state(States.positions)
    text = "Открытые позиции"
    # получаем тикеры для расчета PNL
    ticker_price = await client.ticker_price()
    # перебираем все открытые позиции
    for trade in (await get_positions()):
        # перебираем цены тикеров
        for ticker in ticker_price:
            # если нашли нужный тикер
            if ticker['symbol'] == trade.symbol:
                # запоминаем цену
                t_price = float(ticker['price'])
                break
        else:
            t_price = 0
        # вычисляем PNL
        pnl = (t_price - trade.entry_price) * trade.quantity if t_price else 0
        # формируем текст
        text += (f"\n\n<b>ЛОНГ</b> по #{trade.symbol}\n"
                 f"Размер позиции: <b>{trade.quantity}</b>\n"
                 f"Цена входа: <b>{trade.entry_price}</b>\n"
                 f"Стоп/Тейк: <b>{trade.stop_price} / {trade.take_price}</b>\n"
                 f"PNL: <b>{pnl:.2f}</b>")
    # если вызов был из callback
    if isinstance(message, CallbackQuery):
        # отвечаем на callback
        await message.answer()
        # получаем message
        message = message.message
    # если есть открытые позиции
    if "\n" in text:
        # создаем клавиатуру
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Закрыть позицию", callback_data="close_pos")],
            [InlineKeyboardButton(text="Закрыть все позиции", callback_data="close_pos_all")],
            [InlineKeyboardButton(text="Обновить", callback_data="positions")]
        ])
        # отправляем сообщение
        await message.answer(text, reply_markup=keyboard)
    else:
        # если нет открытых позиции
        # отправляем сообщение
        await message.answer("Открытых позиций нет")


# функция для закрытия позиции
@dp.callback_query(States.positions, F.data == "close_pos")
async def close_pos(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для закрытия позиций
    await state.set_state(States.close_pos)
    # формируем клавиатуру
    keyboard = []
    # перебираем все открытые позиции
    for trade in (await get_positions()):
        # добавляем кнопку для закрытия позиции
        keyboard.append([InlineKeyboardButton(text=trade.symbol, callback_data=f"close_pos:{trade.symbol}")])
    # добавляем кнопку для отмены
    keyboard.append([InlineKeyboardButton(text="Отмена", callback_data="positions")])
    # отвечаем на callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer("Выберите торговую пару, которую хотите закрыть",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# функция для закрытия позиции
@dp.callback_query(States.close_pos, F.data.startswith("close_pos:"))
async def close_pos_yes(callback: CallbackQuery, state: FSMContext):
    # получаем торговую пару
    symbol = callback.data.split(":")[1]
    # если нужно закрыть все позиции
    if symbol == 'all':
        await callback.answer(f"Закрываю все позиции")
        # закрываем все позиции
        await close_position()
    else:
        await callback.answer(f"Закрываю позицию по паре {symbol}")
        # закрываем одну позицию
        await close_position(symbol)
    # перезагружаем позиции
    await open_positions(callback.message, state)


# функция для закрытия всех позиций
@dp.callback_query(States.positions, F.data == "close_pos_all")
async def close_pos_all(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для закрытия позиций
    await state.set_state(States.close_pos)
    # создаем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="close_pos:all")],
        [InlineKeyboardButton(text="Нет", callback_data="positions")]
    ])
    # отвечаем на callback
    await callback.answer()
    # отправляем сообщение
    await callback.message.answer("Вы действительно хотите закрыть все позиции?", reply_markup=keyboard)