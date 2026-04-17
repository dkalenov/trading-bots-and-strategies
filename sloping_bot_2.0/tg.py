import asyncio
import configparser
import binance
import db
import os
from pathlib import Path
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
BASE_DIR = Path(__file__).resolve().parent
# загружаем конфиг
config = configparser.ConfigParser()
config.read(BASE_DIR / 'config.ini')
# парсим список админов
tg_admins = list(map(int, config['TG']['admins'].split(',')))

# описание параметров настроек
symbol_settings = {
    'interval': 'интервал',
    'order_size': 'размер ордера (фолбэк)',
    'leverage': 'плечо',
    'length': 'длина окна для наклонок',
    'atr_length': 'длина для ATR',
    'take1': 'тейк 1 (ATR)',
    'take2': 'тейк 2 (ATR)',
    'stop': 'стоп (ATR)',
    'portion': 'доля закрытия по TP1',
    'risk_pct': 'риск на сделку (%)',
    'max_positions': 'макс. позиций',
    'min_space': 'min interval between signals',
    'min_touches': 'min line touches',
    'breakout_buffer': 'breakout buffer (ATR)',
    'slope_filter': 'slope filter (0/1)',
    'use_trend_filter': 'SMA trend filter (0/1)',
    'trend_sma': 'SMA length for trend',
    'vol_filter': 'volatility filter (0/1)',
    'vol_lookback': 'vol lookback bars',
    'trail_after_tp1': 'trailing stop after TP1 (0/1)',
    'trail_atr': 'trailing distance (ATR)',
}

# список интервалов
intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

# параметры, при обновлении которых нужно перезагружать индикатор
symbol_reload = ['interval', 'length', 'atr_length', 'min_space', 'min_touches',
                 'breakout_buffer', 'slope_filter', 'use_trend_filter', 'trend_sma',
                 'vol_filter', 'vol_lookback']


# фунция для запуска бота
async def run(_session, _client: binance.Futures, _connect_ws, _disconnect_ws, _subscribe_ws, _unsubscribe_ws,
              _remove_indicator):
    # передаем функции и параметры из файла main
    global bot
    global dp
    global session
    global client
    global connect_ws
    global disconnect_ws
    global subscribe_ws
    global unsubscribe_ws
    global remove_indicator
    session = _session
    client = _client
    connect_ws = _connect_ws
    disconnect_ws = _disconnect_ws
    subscribe_ws = _subscribe_ws
    unsubscribe_ws = _unsubscribe_ws
    remove_indicator = _remove_indicator
    # инициализируем бота
    bot = Bot(token=config['TG']['token'], default=DefaultBotProperties(parse_mode='HTML'))
    # удаляем старые сообщения
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        # запускаем бота
        await dp.start_polling(bot)
    finally:
        # закрываем сессию
        await bot.session.close()


# класс для хранения состояний бота
class States(StatesGroup):
    main_menu = State()
    positions = State()
    symbols = State()
    settings = State()
    edit_symbol = State()
    edit_symbol_status = State()
    add_symbol = State()
    delete_symbol = State()
    change_keys = State()
    restart = State()
    trade_mode = State()
    close_pos = State()


# функция для ответа на сообщение или коллбек
async def answer(message: Message | CallbackQuery, text, reply_markup=None):
    if isinstance(message, CallbackQuery):
        await message.answer()
        message = message.message
    await message.answer(text, reply_markup=reply_markup)


# функция для пропуска команд от других людей (не от админов)
@dp.message(~F.from_user.id.in_(tg_admins))
async def skip(_):
    pass


# главное меню
@dp.message(Command("start", "menu"))
@dp.message(F.text == "Главное меню")
async def start(message: Message, state: FSMContext):
    # устанавливаем состояние
    await state.set_state(States.main_menu)
    # создаем клавиатуру
    keyboard = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Открытые позиции"), KeyboardButton(text="Торговые пары")],
        [KeyboardButton(text="Настройки"), KeyboardButton(text="Главное меню")]
    ], resize_keyboard=True)
    # отправляем сообщение
    await answer(message, "Главное меню", reply_markup=keyboard)


# торговые пары
@dp.message(F.text == "Торговые пары")
@dp.callback_query(F.data == "symbols")
async def list_symbols(message: Message | CallbackQuery, state: FSMContext):
    # устанавливаем состояние
    await state.set_state(States.symbols)
    # загружаем список торговых пар из базы данных
    symbols = await db.get_all_symbols()
    keyboard = []
    # создаем клавиатуру
    for symbol in symbols:
        keyboard.append([InlineKeyboardButton(text=symbol.symbol, callback_data=f"symbol:{symbol.symbol}")])
    # добавляем кнопку для добавления пары
    keyboard.append([InlineKeyboardButton(text="Добавить пару", callback_data="add_symbol")])
    await answer(message, "Список торговых пар", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# выбор торговой пары
@dp.callback_query(F.data.startswith("symbol:"))
async def symbol(callback: CallbackQuery, state: FSMContext):
    # получаем торговую пару из callback'а
    symbol = callback.data.split(':')[1]
    # записываем торговую пару в состояние
    await state.update_data(symbol=symbol)
    # выводим меню для редактирования пары
    await symbol_menu(callback, state, symbol)


# меню для редактирования пары
async def symbol_menu(message: Message | CallbackQuery, state: FSMContext, symbol):
    # устанавливаем состояние
    await state.set_state(States.symbols)
    # получаем конфигурацию торговой пары
    symbol_conf = await db.get_symbol_conf(symbol)
    # формируем сообщение
    slope_on = '✅' if symbol_conf.slope_filter else '❌'
    trend_on = '✅' if symbol_conf.use_trend_filter else '❌'
    text = (f"Торговая пара: <b>{symbol}</b>\n"
            f"Статус: <b>{'Активна' if symbol_conf.status else 'Не активна'}</b>\n"
            f"Таймфрейм: <b>{symbol_conf.interval}</b>\n"
            f"Плечо: <b>X{symbol_conf.leverage}</b>\n"
            f"Длина окна: <b>{symbol_conf.length}</b>\n"
            f"ATR: <b>{symbol_conf.atr_length}</b>\n"
            f"Тейк 1 / Тейк 2: <b>{symbol_conf.take1} / {symbol_conf.take2} x ATR</b>\n"
            f"Стоп: <b>{symbol_conf.stop} x ATR</b>\n"
            f"Доля закрытия по TP1: <b>{symbol_conf.portion * 100:.0f}%</b>\n"
            f"Риск на сделку: <b>{symbol_conf.risk_pct * 100:.1f}%</b>\n"
            f"Макс. позиций: <b>{symbol_conf.max_positions}</b>\n"
            f"\n<i>Фильтры Sloping:</i>\n"
            f"Min space: <b>{symbol_conf.min_space}</b> | Min touches: <b>{symbol_conf.min_touches}</b>\n"
            f"Буфер пробоя: <b>{symbol_conf.breakout_buffer} ATR</b>\n"
            f"Slope filter: {slope_on} | Trend SMA({symbol_conf.trend_sma}): {trend_on}\n")
    # формируем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'Отключить' if symbol_conf.status else 'Включить'} торговлю",
                              callback_data="edit_symbol_status")],
        [InlineKeyboardButton(text="Изменить настройки", callback_data="edit_symbol_settings")],
        [InlineKeyboardButton(text="Удалить пару", callback_data="delete_symbol")],
        [InlineKeyboardButton(text="Назад", callback_data="symbols")]
    ])
    # отправляем сообщение
    await answer(message, text, reply_markup=keyboard)


# меню для редактирования настройки пары
@dp.callback_query(States.symbols, F.data == "edit_symbol_settings")
async def edit_symbol_settings(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # формируем клавиатуру со списком параметров для редактирования
    keyboard = []
    for key, value in symbol_settings.items():
        keyboard.append([InlineKeyboardButton(text=f"Изменить {value}", callback_data=f"edit_symbol:{key}")])
    keyboard.append([InlineKeyboardButton(text="Назад", callback_data=f"symbol:{data['symbol']}")])
    # отправляем сообщение
    await answer(callback, "Изменить настройки", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# редактирование параметра
@dp.callback_query(States.symbols, F.data.startswith("edit_symbol:"))
async def edit_symbol(callback: CallbackQuery, state: FSMContext):
    # получаем параметр для редактирования
    key = callback.data.split(':')[1]
    # устанавливаем состояние
    await state.set_state(States.edit_symbol)
    # записываем параметр в состояние
    await state.update_data(key=key)
    # загружаем конфиг пары
    data = await state.get_data()
    symbol_conf = await db.get_symbol_conf(data['symbol'])
    # формируем сообщение
    text = (f"Текущее значение {symbol_settings[key]}: <b>{getattr(symbol_conf, key)}</b>\n"
            f"Введите новое значение")
    await answer(callback, text)


# редактирование параметра
@dp.message(States.edit_symbol)
async def edit_symbol_value(message: Message, state: FSMContext):
    # загружаем состояние
    data = await state.get_data()
    # получаем параметр для редактирования
    symbol_conf = await db.get_symbol_conf(symbol=data['symbol'])
    symbol = data['symbol']
    key = data['key']
    # получаем старое значение
    value = getattr(symbol_conf, key)
    # и если оно не равно None
    if value is not None:
        try:
            # пробуем привести новое значение к нужному типу
            value = type(value)(message.text)
            # проверяем, что новое значение больше нуля (если это число)
            if type(value) in (int, float) and value <= 0:
                # отправляем сообщение с ошибкой
                await answer(message, "Значение должно быть больше нуля")
                return
        except:
            # отправляем сообщение с ошибкой
            await answer(message, "Некорректное значение, попробуйте ещё раз")
            return
        match key:
            # если мы изменяем плечо
            case 'leverage':
                try:
                    # пытаемся изменить плечо на бирже
                    await client.change_leverage(symbol, value)
                except Exception as e:
                    # отправляем сообщение с ошибкой
                    await answer(message, f"Не удалось изменить плечо:\n{e}")
                    return
            # если мы изменяем интервал
            case 'interval':
                # проверяем, что новое значение входит в список допустимых значений
                if value not in intervals:
                    # отправляем сообщение с ошибкой
                    await answer(message, "Некорректное значение, допустимые значения: " + ', '.join(intervals))
                    return
            # --- Валидация границ ---
            case 'portion':
                if not (0 < value <= 1):
                    await answer(message, "Доля должна быть от 0 до 1 (напр. 0.5)")
                    return
            case 'risk_pct':
                if not (0 < value <= 0.1):
                    await answer(message, "Риск должен быть от 0 до 0.1 (10%). Рекомендуется 0.01–0.03")
                    return
            case 'length':
                if value > 500:
                    await answer(message, "Длина окна не может превышать 500 (буфер индикатора)")
                    return
            case 'atr_length':
                if value > 500:
                    await answer(message, "ATR длина не может превышать 500")
                    return
            case 'min_space':
                if value < 0 or value > 50:
                    await answer(message, "min_space должен быть от 0 до 50")
                    return
            case 'min_touches':
                if value < 1 or value > 10:
                    await answer(message, "min_touches должен быть от 1 до 10")
                    return
            case 'breakout_buffer':
                if value < 0 or value > 2.0:
                    await answer(message, "breakout_buffer должен быть от 0 до 2.0")
                    return
            case 'slope_filter' | 'use_trend_filter':
                if value not in (0, 1):
                    await answer(message, "Введите 0 (выкл) или 1 (вкл)")
                    return
                value = bool(value)
            case 'trend_sma':
                if value < 10 or value > 500:
                    await answer(message, "trend_sma должен быть от 10 до 500")
                    return
        # устанавливаем новое значение
        setattr(symbol_conf, key, value)
        # сохраняем конфиг пары
        await db.symbol_update(symbol_conf)
        # если параметр входит в список параметров, при изменении которых нужно перезагружать индикатор
        if key in symbol_reload:
            # если мы изменили интервал
            if key == 'interval':
                # отписываемся от предыдущего стрима
                await unsubscribe_ws(symbol)
                # удаляем индикатор, чтобы он перезагрузился
                await remove_indicator(symbol)
                # подписываемся на новый стрим
                await subscribe_ws(symbol, symbol_conf.interval)
            else:
                # удаляем индикатор, чтобы он перезагрузился
                await remove_indicator(symbol)
        # выводим сообщение
        await answer(message, f"Новое значение {symbol_settings[key]} установлено: <b>{value}</b>")
        # отправляем меню
        await symbol_menu(message, state, symbol)
    else:
        await answer(message, "Некорректное значение, попробуйте ещё раз")


# изменение статуса торговли
@dp.callback_query(States.symbols, F.data == "edit_symbol_status")
async def edit_symbol_status(callback: CallbackQuery, state: FSMContext):
    # загружаем состояние
    data = await state.get_data()
    # устанавливаем состояние
    await state.set_state(States.edit_symbol_status)
    # загружаем конфиг пары
    symbol_conf = await db.get_symbol_conf(symbol=data['symbol'])
    # создаем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="edit_symbol_status_yes"),
         InlineKeyboardButton(text="Нет", callback_data=f"symbol:{data['symbol']}")]
    ])
    # отправляем сообщение
    await answer(callback, f"Вы уверены, что хотите {'отключить' if symbol_conf.status else 'включить'} торговлю"
                           f" для пары <b>{data['symbol']}</b>?", reply_markup=keyboard)


# изменение статуса торговли
@dp.callback_query(States.edit_symbol_status, F.data == "edit_symbol_status_yes")
async def edit_symbol_status_yes(callback: CallbackQuery, state: FSMContext):
    # загружаем состояние
    data = await state.get_data()
    symbol = data['symbol']
    # загружаем конфиг пары
    symbol_conf = await db.get_symbol_conf(symbol=symbol)
    # изменяем статус
    symbol_conf.status = 0 if symbol_conf.status else 1
    # записываем в базу
    await db.symbol_update(symbol_conf)
    # отписываемся от стрима, если торговля отключена
    if not symbol_conf.status:
        # Предупреждение если есть открытая позиция
        open_trade = await db.get_open_trade(symbol)
        if open_trade:
            await answer(callback,
                         f"⚠️ Торговля отключена для <b>{symbol}</b>, но есть открытая позиция.\n"
                         f"TP1 мониторинг остановлен. Стоп/TP2 остаются на бирже.")
        await unsubscribe_ws(symbol)
    # удаляем старый индикатор
    await remove_indicator(symbol)
    # подписываемся на стрим, если торговля включена
    if symbol_conf.status:
        await subscribe_ws(symbol, symbol_conf.interval)
    await answer(callback, f"Торговля для пары <b>{symbol}</b> успешно "
                           f"{'включена' if symbol_conf.status else 'отключена'}")
    await symbol_menu(callback, state, symbol)


# добавление пары
@dp.callback_query(States.symbols, F.data == "add_symbol")
async def add_symbol(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние
    await state.set_state(States.add_symbol)
    # отправляем сообщение
    await answer(callback, "Введите название пары")


# добавление пары
@dp.message(States.add_symbol)
async def add_symbol_value(message: Message, state: FSMContext):
    # переводим в верхний регистр
    symbol = message.text.upper()
    # проверяем есть ли такая пара на бирже
    if symbol not in (await client.load_symbols()):
        # если нет, то отправляем сообщение об ошибке
        await answer(message, f"Торговой пары {symbol} не существует, попробуйте еще раз")
        return
    # добавляем пару в базу данных
    await db.symbol_add(symbol)
    # записываем в состояние
    await state.update_data(symbol=symbol)
    # формируем сообщение и пытаемся изменить плечо на бирже
    text = f"Торговая пара <b>{symbol}</b> успешно добавлена"
    try:
        await client.change_leverage(symbol, 20)
    except:
        text += f", но плечо изменить не удалось"
    # отправляем сообщение
    await answer(message, text)
    # отправляем меню
    await symbol_menu(message, state, symbol)


# удаление пары
@dp.callback_query(States.symbols, F.data == "delete_symbol")
async def delete_symbol(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние
    await state.set_state(States.delete_symbol)
    # загружаем данные
    data = await state.get_data()
    # создаем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="delete_symbol_yes"),
         InlineKeyboardButton(text="Нет", callback_data=f"symbol:{data['symbol']}")]
    ])
    # отправляем сообщение
    await answer(callback, f"Вы уверены, что хотите удалить пару <b>{data['symbol']}</b>?", reply_markup=keyboard)


# удаление пары
@dp.callback_query(States.delete_symbol, F.data == "delete_symbol_yes")
async def delete_symbol_yes(callback: CallbackQuery, state: FSMContext):
    # загружаем состояние
    data = await state.get_data()
    # получаем пару
    symbol = data['symbol']
    # удаляем пару из базы данных
    await db.symbol_delete(symbol)
    # отписываемся от стрима
    await unsubscribe_ws(symbol)
    # удаляем индикатор
    await remove_indicator(symbol)
    # отправляем сообщение
    await answer(callback, f"Пара <b>{symbol}</b> успешно удалена")
    # отправляем меню
    await list_symbols(callback, state)


# загрузка списка позиций
async def get_positions():
    trades = {}
    # загружаем трейды из базы данных
    for trade in (await db.get_open_trades()):
        # и формируем словарь
        trades[trade.symbol] = trade
    positions = []
    # загружаем позиции с биржи
    for pos in (await client.get_position_risk()):
        # выбираем у которых размер позиции не равен нулю и они есть в нашей базе данных
        if float(pos['positionAmt']) != 0 and pos['symbol'] in trades:
            # добавляем в список
            positions.append(pos)
    # возвращаем результат
    return positions, trades


@dp.message(F.text == "Открытые позиции")
@dp.callback_query(F.data == "positions")
async def open_positions(message: Message, state: FSMContext):
    # загружаем кофиг
    conf = await db.load_config()
    # если торговля отключена
    if not conf.trade_mode:
        # отправляем сообщение
        await answer(message, "Торговля отключена")
        return
    # устанавливаем состояние
    await state.set_state(States.positions)
    text = "Открытые позиции:"
    positions, trades = await get_positions()
    for pos in positions:
        size = float(pos['positionAmt'])
        trade = trades[pos['symbol']]
        # формируем сообщение
        tp1_status = '✅' if trade.take1_triggered else ''
        tp2_display = trade.take2_price if trade.take2_price else 'нет'
        text += (f"\n\n<b>{'ЛОНГ' if size > 0 else 'ШОРТ'}</b> <i>X{pos['leverage']}</i> #{pos['symbol']}\n"
                 f"Размер позиции: <b>{abs(size)}</b>\n"
                 f"Цена входа: <b>{pos['entryPrice']}</b>\n"
                 f"Стоп: <b>{trade.breakeven_stop_price or trade.stop_price}</b>\n"
                 f"TP1: <b>{trade.take1_price}</b> {tp1_status} / TP2: <b>{tp2_display}</b>\n"
                 f"PNL: <b>{round(float(pos['unRealizedProfit']), 2)} USDT</b>")
    # если есть позиции
    if "\n" in text:
        # создаем клавиатуру
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Закрыть позицию", callback_data="close_pos")],
            [InlineKeyboardButton(text="Закрыть все позиции", callback_data="close_pos_all")],
            [InlineKeyboardButton(text="Обновить", callback_data="positions")]
        ])
        # отправляем сообщение
        await answer(message, text, reply_markup=keyboard)
    else:
        await answer(message, "Открытых позиций нет")


# настройки
@dp.message(F.text == "Настройки")
@dp.callback_query(F.data == "settings")
async def settings(message: Message, state: FSMContext):
    # загружаем настройки из базы данных
    conf = await db.load_config()
    # устанавливаем состояние
    await state.set_state(States.settings)
    # создаем клавиатуру
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'Отключить' if conf.trade_mode else 'Включить'} торговлю",
                              callback_data="trade_mode")],
        [InlineKeyboardButton(text="Изменить ключи", callback_data="change_keys")],
        [InlineKeyboardButton(text="Перезагрузить бота", callback_data="restart")]
    ])
    # формируем сообщение
    api_key = f"{conf.api_key[:4]}...{conf.api_key[-4:]}" if conf.api_key else "Отсутствует"
    text = (f"Основные настройки:\n"
            f"Торговля: <b>{'Включена' if conf.trade_mode else 'Отключена'}</b>\n"
            f"API KEY: <b>{api_key}</b>\n")
    # отправляем сообщение
    await answer(message, text, reply_markup=keyboard)


# изменение ключей
@dp.callback_query(States.settings, F.data == "change_keys")
async def change_keys(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для изменения ключей
    await state.set_state(States.change_keys)
    # отвечаем на callback
    await callback.answer()
    # запрашиваем API KEY
    await callback.message.answer("Введите API KEY:")


# функция для изменения ключей
@dp.message(States.change_keys)
async def change_keys_value(message: Message, state: FSMContext):
    # получаем состояние
    data = await state.get_data()
    # если API KEY еще не был введен
    if not data.get('api_key'):
        # записываем API KEY
        await state.update_data(api_key=message.text)
        # запрашиваем SECRET KEY
        await answer(message, "Введите SECRET KEY:")
        # удаляем сообщение с API KEY
        await message.delete()
        return
    else:
        # берем API KEY и SECRET KEY
        api_key, secret_key = data['api_key'], message.text
        # пробуем подключиться с ними к бирже и проверить корректность
        tmp_client = binance.Futures(api_key, secret_key, asynced=True,
                                     testnet=config.getboolean('BOT', 'testnet'))
        try:
            # делаем запрос к информации об аккаунте
            await tmp_client.account()
        except:
            # если ключи некорректны, пишем об этом
            await answer(message, f"Ключи некорректны, попробуйте ещё раз\nВведите API KEY:")
            # удаляем API KEY из памяти (чтобы можно было ввести еще раз)
            await state.update_data(api_key=None)
            return
        finally:
            # в любом случае закрываем соединение клиента
            await tmp_client.close()
            # удаляем сообщение с SECRET KEY
            await message.delete()
        await db.config_update(api_key=api_key, api_secret=secret_key)
        # отправляем сообщение
        await answer(message, "Ключи успешно изменены, перезагружаю бота")
        # перезагружаю бота
        await restart_yes(message, state)


@dp.callback_query(F.data == "restart")
async def restart(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для подтверждения перезагрузки
    await state.set_state(States.restart)
    # формируем клавиатуру с подтверждением удаления
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data=f"restart_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="settings")]
    ])
    await callback.answer()
    # отправляем сообщение с подтверждением
    await callback.message.answer(f"Вы действительно хотите перезагрузить бота?", reply_markup=keyboard)


# функция перезагрузки бота
@dp.callback_query(States.restart, F.data == "restart_yes")
async def restart_yes(callback: CallbackQuery | Message, state: FSMContext):
    try:
        # закрываем позиции
        await close_position()
        # отправляем сообщение о перезагрузке
        if isinstance(callback, CallbackQuery):
            await callback.message.answer('Перезагрузка...')
        else:
            await callback.answer('Перезагрузка...')
    finally:
        # перезагружаем бота (просто завершаем процесс, systemd на сервере сам перезапустит сервис)
        os._exit(0)


@dp.callback_query(F.data == "trade_mode")
async def trade_mode(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для подтверждения перезагрузки
    await state.set_state(States.trade_mode)
    # загружаем конфиг
    conf = await db.load_config()
    # формируем клавиатуру с подтверждением удаления
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data=f"trade_mode_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="settings")]
    ])
    await callback.answer()
    # отправляем сообщение с подтверждением
    await callback.message.answer(f"Вы действительно хотите {'включить' if not conf.trade_mode else 'отключить'} "
                                  f"торговлю?", reply_markup=keyboard)


# функция подтверждения включения или выключения торговли
@dp.callback_query(States.trade_mode, F.data == "trade_mode_yes")
async def trade_mode_yes(callback: CallbackQuery, state: FSMContext):
    # загружаем конфиг
    conf = await db.load_config()
    # включаем или выключаем торговлю
    conf.trade_mode = 0 if conf.trade_mode else 1
    # проверяем есть ли у нас ключи (если мы пытаемся включить торговлю)
    if conf.trade_mode and (not conf.api_key or not conf.api_secret):
        # отвечаем на callback
        await callback.answer()
        # отправляем сообщение об ошибке
        await callback.message.answer("Для включения торговли необходимо ввести API KEY и SECRET KEY")
        return
    # обновляем конфиг
    await db.config_update(trade_mode=str(conf.trade_mode))
    if conf.trade_mode:
        # подключаемся к вебсокетам
        await connect_ws()
    else:
        # закрываем все позиции
        await close_position()
        # отключаемся от вебсокетов
        await disconnect_ws()
    # отправляем сообщение
    await callback.answer('Торговля включена' if conf.trade_mode else 'Торговля выключена')
    # возвращаемся в меню настроек
    await settings(callback.message, state)


# закрытие позиции
@dp.callback_query(F.data == "close_pos")
async def close_pos(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для закрытия позиций
    await state.set_state(States.close_pos)
    # получаем открытые позиции
    positions, _ = await get_positions()
    # формируем клавиатуру с выбором торговой пары для закрытия позиции
    keyboard = []
    for pos in positions:
        keyboard.append([InlineKeyboardButton(text=pos['symbol'], callback_data=f"close_pos_yes:{pos['symbol']}")])
    keyboard.append([InlineKeyboardButton(text="Отмена", callback_data="positions")])
    # формируем клавиатуру с подтверждением закрытия всех позиций
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await callback.answer()
    # отправляем сообщение с подтверждением
    await callback.message.answer(f"Выберите торговую пару, которую вы хотите закрыть", reply_markup=keyboard)


# закрытие всех позиций
@dp.callback_query(F.data == "close_pos_all")
async def close_pos_all(callback: CallbackQuery, state: FSMContext):
    # устанавливаем состояние для закрытия позиций
    await state.set_state(States.close_pos)
    # формируем клавиатуру с подтверждением закрытия всех позиций
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data=f"close_pos_yes:all")],
        [InlineKeyboardButton(text="Нет", callback_data="positions")]
    ])
    await callback.answer()
    # отправляем сообщение с подтверждением
    await callback.message.answer(f"Вы действительно закрыть все позиции?", reply_markup=keyboard)

# функция для подтверждения закрытия позиции
@dp.callback_query(States.close_pos, F.data.startswith("close_pos_yes:"))
async def close_pos_yes(callback: CallbackQuery, state: FSMContext):
    # получаем символ торговой пары
    symbol = callback.data.split(":")[1]
    # если нужно закрыть все позиции
    if symbol == 'all':
        await callback.answer("Закрываю все позиции")
        # закрываем все позиции
        await close_position()
    # если только одну позицию
    else:
        await callback.answer(f"Закрываю позицию по паре {symbol}")
        # закрываем позицию
        await close_position(symbol)
    # выводим список открытых позиций
    await open_positions(callback.message, state)


# функция для закрытия позиции
async def close_position(symbol=None):
    # получаем открытые позиции
    positions, _ = await get_positions()
    # перебираем позиции
    tasks = []
    for pos in positions:
        # если пара не указана, закрываем все позиции, в противном случае закрываем только нужную позицию
        if not symbol or pos['symbol'] == symbol:
            # размер позиции
            pos_amt = float(pos['positionAmt'])
            # определяем направление ордера для закрытия (BUY или SELL)
            side = 'SELL' if pos_amt > 0 else 'BUY'
            # создаем задачу для закрытия позиции
            tasks.append(asyncio.create_task(close_pos_order(pos['symbol'], side, abs(pos_amt))))
    # ожидаем завершения всех задач
    await asyncio.gather(*tasks)


# функция для отправки ордера на закрытие позиции
async def close_pos_order(symbol, side, qty):
    # создаем рыночный ордер на закрытие позиции
    await client.new_order(symbol=symbol, side=side, type='MARKET', quantity=qty, reduceOnly="True")
