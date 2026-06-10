import configparser
import binance
import db
import tg
import ob
import asyncio
import multiprocessing
import traceback
import utils
import time


# класс для хранения позиции
class Position:
    symbol: str
    quantity: float
    entry_price: float
    order_id: int
    status: bool
    commission: float
    qty: float
    coin: str
    density: float
    density_volume: float
    db_ready: bool
    fill_time: float
    pending_exit_task: asyncio.Task
    pending_exit_reason: str
    tp_order_id: int
    sl_order_id: int
    exit_order_id: int
    tg_message_id: int
    tg_task: asyncio.Task
    atr: float
    tp_price: float
    stop_price: float

    def __init__(self, symbol, quantity, entry_price, density, density_volume=0.0, atr=None):
        self.symbol = symbol
        self.quantity = quantity
        self.entry_price = entry_price
        self.order_id = 0
        self.status = False
        self.commission = 0
        self.qty = 0
        self.density = density
        self.density_volume = density_volume
        self.db_ready = False
        self.fill_time = 0.0
        self.pending_exit_task = None
        self.pending_exit_reason = ''
        self.tp_order_id = None
        self.sl_order_id = None
        self.exit_order_id = None
        self.tg_message_id = None
        self.tg_task = None
        self.atr = atr
        self.tp_price = None
        self.stop_price = None
        self.be_activated = False
        # получаем базовый актив торговой пары (например BTC для BTCUSDT)
        for sym in all_symbols.values():
            if sym.symbol == symbol:
                self.coin = sym.base_asset
        else:
            self.coin = ''


# загружаем конфиг
config = configparser.ConfigParser()
config.read('config.ini')
# словарь для хранения торговых пар
all_symbols: dict[str, binance.symbols.SymbolFutures] = {}
client = None
conf = None
session = None
# список для процессов
processes = []
# очередь для передачи данных плотностей из процессов вебсокетов
main_queue = multiprocessing.Queue()
# очередь для передачи данных из телеграм бота
tg_queue = asyncio.Queue()
# словарь для хранения информации и последней плотности (для автоматической отмены ордера при уходе из плотности)
symbols_densities = {}
# переменная для хранения баланса
balance = 0.0
# переменная для хранения цены BNB (для расчета комиссий)
bnb_price = 0.0
# словарь для хранения объемов за 24 часа
volumes_24h = {}
# словарь для хранения последних цен тикеров
last_prices = {}
# словарь для хранения истории цен тикеров за последние 15 минут
price_history = {}
# словарь для хранения позиций
positions: dict[str, Position] = {}
# вебсокет для получения тикеров
ticker_ws = None
# вебсокет userdata
userdata_ws = None
# словарь для хранения исходов последних сделок по каждому символу (v8.3+)
# формат: {symbol: [(close_time_ms, is_win), ...]}
symbol_recent_trades = {}
pending_entry_confirmations = {}


ENTRY_CONFIRMATION_DELAY = 5.0
EXIT_DEBOUNCE_SECONDS = 2.0
MIN_POSITION_HOLD_SECONDS = 3.0
MIN_REWARD_RISK_RATIO = 1.5
MAX_SPREAD_PCT = 0.05
MAX_DROP_5M_PCT = -0.4
MAX_DROP_15M_PCT = -0.7
MIN_CONFIRM_RETAINED_RATIO = 0.7
MAKER_FEE_RATE = 0.0002
TAKER_FEE_RATE = 0.0005
MIN_NET_REWARD_RISK_RATIO = 1.25
MIN_OPPORTUNITY_SCORE_C = 5.5
MIN_OPPORTUNITY_SCORE_D = 7.0
MAX_ENTRY_DISTANCE_PCT = 0.50

# Глобальные тренд-фильтры по BTC и ETH
BTC_MAX_DROP_5M_PCT = -0.15
BTC_MAX_DROP_15M_PCT = -0.3
ETH_MAX_DROP_5M_PCT = -0.2
ETH_MAX_DROP_15M_PCT = -0.4

# Ограничение на время удержания сделки (5 минут)
MAX_HOLD_TIME_SECONDS = 300

# Порог разъедания плотности (выход, если осталось менее 15% объема)
DENSITY_EATEN_THRESHOLD_PCT = 0.15


def parse_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
    return bool(value)


def instant_exits_enabled():
    if conf is not None and hasattr(conf, 'enable_instant_exits'):
        return parse_bool(getattr(conf, 'enable_instant_exits'))
    try:
        return config.getboolean('BOT', 'ENABLE_INSTANT_EXITS', fallback=False)
    except Exception:
        return False


# функция для инициализации кэша истории сделок (v8.3+)
async def init_trade_history_cache():
    global symbol_recent_trades
    print("[Бот] Инициализация кэша истории сделок...")
    try:
        async with session() as s:
            from sqlalchemy import select
            # Получаем все закрытые сделки, упорядоченные по времени закрытия по возрастанию
            result = await s.execute(
                select(db.Trades)
                .where(db.Trades.status.in_(['CLOSED_TAKE', 'CLOSED_STOP', 'CLOSED_MARKET']))
                .order_by(db.Trades.close_time.asc())
            )
            trades = result.scalars().all()
            for t in trades:
                if t.symbol not in symbol_recent_trades:
                    symbol_recent_trades[t.symbol] = []
                is_win = (t.profit is not None and t.profit > 0)
                symbol_recent_trades[t.symbol].append((t.close_time, is_win))
                if len(symbol_recent_trades[t.symbol]) > 10:
                    symbol_recent_trades[t.symbol].pop(0)
        print(f"[Бот] Кэш истории сделок успешно загружен. Найдено монет с историей: {len(symbol_recent_trades)}")
    except Exception as e:
        print(f"[Бот] Ошибка при инициализации кэша истории сделок: {e}")


# функция запуска бота
async def main():
    # глобальные переменные
    global client
    global all_symbols
    global conf
    global session
    print("[Бот] Подключение к базе данных PostgreSQL...")
    # подключаемся к базе данных
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                               config['DB']['password'], config['DB']['db'])
    print("[Бот] Успешное подключение к базе данных.")
    # загружаем конфиг
    conf = await db.load_config()
    print(f"[Бот] Конфигурация загружена. Режим торговли: {'Включен' if conf.trade_mode else 'Выключен'}. Режим симуляции (Dry Run): {'Да' if conf.dry_run else 'Нет'}.")
    # создаем клиент
    client = binance.Futures(conf.api_key, conf.api_secret, asynced=True,
                          testnet=config.getboolean('BOT', 'TESTNET'))
    # загружаем все символы
    asyncio.create_task(load_symbols())
    # проверяем API KEY и SECRET KEY (только если не в режиме симуляции)
    if not conf.dry_run and (not conf.api_key or not conf.api_secret):
        # если нет, то отключаем торговлю
        await db.config_update(trade_mode='0')
        conf = await db.load_config()
        print("[Бот] Внимание: отсутствуют API ключи для реальной торговли! Режим торговли принудительно отключен.")
    # запускаем функцию получения плотностей от вебсокетов
    asyncio.create_task(get_density())
    # запускаем функцию получения данных от телеграм бота
    asyncio.create_task(get_tg_data())
    # запускаем функцию проверки позиции
    asyncio.create_task(check_positions())
    print("[Бот] Загрузка торговых пар с биржи...")
    # ждем пока не будут загружены все пары
    while not all_symbols:
        await asyncio.sleep(0.1)
    print(f"[Бот] Торговые пары успешно загружены (Всего: {len(all_symbols)}).")
    # Сверяем и восстанавливаем открытые позиции
    await reconcile_state()
    # Инициализация кэша истории сделок (v8.3+)
    await init_trade_history_cache()
    # запускаем процессы вебсокетов
    asyncio.create_task(connect_ws())
    print("[Бот] Запуск Telegram-бота...")
    # запускаем телеграмм бота
    await tg.run(conf, client, tg_queue)


# сервис для загрузки всех торговых пар каждый час
async def load_symbols():
    global all_symbols
    # вечный цикл
    while True:
        try:
            # загружаем все торговые пары
            all_symbols = await client.load_symbols()
        except asyncio.CancelledError:
            try:
                # останавливаем tg бота при завершении работы
                await tg.dp.stop_polling()
            except:
                pass
            break
        except:
            pass
        # ждем 1 час
        await asyncio.sleep(3600)


# функция для получения баланса при запуске бота
async def load_balance():
    global balance
    # получаем балансы
    account = await client.account()
    # перебираем активы
    for asset in account.get('assets', []):
        # если нужный нам актив
        if asset['asset'] == config['BOT']['ASSET']:
            # записываем баланс (доступный баланс)
            balance = float(asset['availableBalance'])
            print("Баланс: ", balance)


# функция для получения данных от телеграм бота
async def get_tg_data():
    # вечный цикл
    while True:
        try:
            # получаем данные
            data = await tg_queue.get()
            # обрабатываем полученные данные
            await tg_data(data)
        except:
            pass


# функция для обработки полученных данных
async def tg_data(data):
    global conf
    # выбираем в зависимости от типа данных название команды
    match data if isinstance(data, str) else data[0]:
        case 'reload_config':
            # перезагружаем конфиг
            conf = await db.load_config()
        case 'reconnect_ws':
            # отключаем вебсокеты
            await disconnect_ws()
            # подключаем вебсокеты
            await connect_ws()
        case 'close_position':
            # закрываем позиции
            await close_position(data[1])


# функция для запуска процессов вебсокетов
async def connect_ws():
    # глобальная переменная процессов
    global processes
    global ticker_ws
    global userdata_ws
    global volumes_24h
    # если торговля отключена, то выходим
    if not conf.trade_mode:
        return
    # проверяем API KEY и SECRET KEY (только если не в режиме симуляции)
    if not conf.dry_run and (not conf.api_key or not conf.api_secret):
        # если нет, то отключаем торговлю
        await db.config_update(trade_mode='0')
        return
    print("Запускаю вебсокеты")

    # Получаем 24-часовые объемы через REST API перед запуском вебсокетов для фильтрации мусорных пар
    retries = 3
    for attempt in range(retries):
        try:
            print(f"[Бот] Получение 24-часовых объемов торгов через REST API (попытка {attempt+1}/{retries})...")
            tickers = await client.ticker_24hr_price_change()
            for t in tickers:
                volumes_24h[t['symbol']] = float(t['quoteVolume'])
            print(f"[Бот] Успешно загружено объемов для {len(volumes_24h)} пар.")
            break
        except Exception as e:
            print(f"[Бот] Ошибка при получении объемов торгов на попытке {attempt+1}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2)
            else:
                print("[Бот] Предупреждение: Не удалось загрузить объемы торгов после нескольких попыток. Используются старые или пустые объемы.")
    
    if not conf.dry_run:
        # загружаем баланс
        asyncio.create_task(load_balance())
        # запускаем вебсокет userdata
        userdata_ws = await client.websocket_userdata(on_message=msg_userdata, on_error=ws_error)
    else:
        # В режиме симуляции (Dry Run) используем виртуальный баланс
        global balance
        if conf.api_key and conf.api_secret:
            try:
                # Попробуем загрузить реальный баланс для реализма
                account = await client.account()
                for asset in account.get('assets', []):
                    if asset['asset'] == config['BOT']['ASSET']:
                        balance = float(asset['availableBalance'])
                        print(f"[DRY RUN] Загружен реальный баланс для симуляции: {balance} {config['BOT']['ASSET']}")
                        break
                else:
                    balance = 10000.0
            except:
                balance = 10000.0
        else:
            balance = 10000.0
        print(f"[DRY RUN] Начальный баланс симуляции: {balance} {config['BOT']['ASSET']}")
        
    # запускаем вебсокет для получения тикеров
    ticker_ws = await client.websocket("!miniTicker@arr", on_message=msg_ticker, on_error=ws_error)
    symbols = []
    # получаем список активных бессрочных (PERPETUAL) контрактов с нужным нам quote_asset,
    # исключая индексы, доминирование и токены UP/DOWN
    stable_fiats = ('USDC', 'FDUSD', 'TUSD', 'EUR', 'BUSD', 'AEUR', 'DAI', 'USDP', 'USDE', 'PAXG', 'XAUT')
    chinese_trash = ('NEO', 'QTUM', 'CFX', 'CKB', 'VET', 'ONT', 'ACH', 'BTT', 'SATS', 'RATS', 'ORDI', 'WUKONG', 'PEIPEI', 'JST')
    
    for sym in all_symbols.values():
        symbol_name = sym.symbol
        
        # Базовые проверки статуса и типа контракта
        if not (sym.status == 'TRADING' 
                and sym.quote_asset == config['BOT']['ASSET'] 
                and getattr(sym, 'contract_type', '') == 'PERPETUAL'
                and getattr(sym, 'underlying_type', '') != 'INDEX'
                and symbol_name.isascii()):
            continue
            
        # Убираем USDC и другие стейблкоины/фиатные пары (везде в названии)
        if any(stable in symbol_name for stable in stable_fiats):
            continue
            
        # Убираем монеты из черного списка
        if symbol_name in conf.blacklist:
            continue
            
        # Убираем китайские названия, BRC-20
        # Если символ начинается на 1000, 1000000, 1M (например, 1000SATS, 1000RATS), мы его оставляем
        is_multiplier = symbol_name.startswith(('1000', '1000000', '1M'))
        if not is_multiplier:
            if any(china in symbol_name for china in chinese_trash):
                continue
            
        # Убираем другие известные мусорные токены / индексы
        if any(x in symbol_name for x in ('BTCDOM', 'DEFI', 'FOOTBALL', 'BLUEBIRD', 'UPUSDT', 'DOWNUSDT')):
            continue
            
        # Фильтруем мусорные/низколиквидные монеты на этапе подписки
        vol_24h = volumes_24h.get(symbol_name, 0.0)
        if vol_24h < conf.min_volume_24h or vol_24h > conf.max_volume_24h:
            continue
            
        symbols.append(sym)
            
    print(f"[Бот] Отфильтровано пар для подписки на вебсокеты: {len(symbols)} из {len(all_symbols)}")
    # разбиваем список на подсписки
    symbols_lists = [symbols[i:i+conf.streams_count] for i in range(0, len(symbols), conf.streams_count)]
    # перебираем подсписки
    for s_list in symbols_lists:
        # создаем процесс
        proc = multiprocessing.Process(target=ob.run, args=(s_list, conf.depth, conf.ask_volume, main_queue,
                                                            config.getboolean('BOT', 'TESTNET'), conf.min_density_num))
        # добавляем процесс в список
        processes.append(proc)
        # запускаем процесс
        proc.start()


# функция для отключения вебсокетов
async def disconnect_ws():
    global processes
    # перебираем процессы
    for proc in processes:
        try:
            # останавливаем процесс
            proc.terminate()
            print(f"Процесс вебсокета №{proc.pid} завершен")
        except:
            pass
    # очищаем список процессов
    processes = []
    # проверяем наличие вебсокета тикеров
    if ticker_ws:
        try:
            # закрываем вебсокет тикеров
            await ticker_ws.close()
        except:
            pass
    if userdata_ws:
        try:
            # закрываем вебсокет userdata
            await userdata_ws.close()
        except:
            pass


# обработка ошибок вебсокета
async def ws_error(ws, error):
    print(f"WS ERROR: {error}\n{traceback.format_exc()}")


# функция для получения цен TP/SL с защитным процентным полом и динамическим TP по классам ликвидности
def get_liquidity_class(symbol):
    vol_24h = volumes_24h.get(symbol, 0.0)
    if vol_24h <= 0.0:
        return None
    if vol_24h >= 1_000_000_000.0:
        return {"name": "Class A", "tp_pct": 0.3, "sl_pct": 0.18, "blocked": True}
    if vol_24h >= 100_000_000.0:
        return {"name": "Class B", "tp_pct": 0.3, "sl_pct": 0.18, "blocked": True}
    if vol_24h >= 10_000_000.0:
        return {"name": "Class C", "tp_pct": 0.70, "sl_pct": 0.35, "blocked": False}
    return {"name": "Class D", "tp_pct": 0.90, "sl_pct": 0.45, "blocked": False}


def get_min_density_floor(symbol, bid_price):
    profile = get_liquidity_class(symbol)
    if profile is None:
        return None, "Unknown"
    if profile["name"] == "Class A":
        return None, profile["name"]
    if profile["name"] == "Class B":
        min_density_floor = 250_000.0
    elif profile["name"] == "Class C":
        min_density_floor = 60_000.0
    else:
        min_density_floor = 30_000.0
    if bid_price > 100.0:
        min_density_floor = min(min_density_floor * (bid_price / 100.0), 2_000_000.0)
    return min_density_floor, profile["name"]


def is_reward_risk_ok(entry_price, take_price, stop_price):
    reward = take_price - entry_price
    risk = entry_price - stop_price
    if reward <= 0 or risk <= 0:
        return False, 0.0
    ratio = reward / risk
    return ratio >= MIN_REWARD_RISK_RATIO, ratio


def estimate_net_reward_risk(entry_price, take_price, stop_price):
    reward_pct = (take_price - entry_price) / entry_price * 100
    risk_pct = (entry_price - stop_price) / entry_price * 100
    net_reward_pct = reward_pct - (MAKER_FEE_RATE + MAKER_FEE_RATE) * 100
    net_risk_pct = risk_pct + (MAKER_FEE_RATE + TAKER_FEE_RATE) * 100
    if net_reward_pct <= 0 or net_risk_pct <= 0:
        return False, 0.0, net_reward_pct, net_risk_pct
    net_ratio = net_reward_pct / net_risk_pct
    return net_ratio >= MIN_NET_REWARD_RISK_RATIO, net_ratio, net_reward_pct, net_risk_pct


def is_maker_entry_safe(entry_price, best_ask, tick):
    if best_ask is None:
        return True, ""
    if entry_price >= best_ask:
        return False, f"entry {entry_price} would cross best ask {best_ask}"
    if best_ask - entry_price < tick * 0.5:
        return False, f"entry {entry_price} is too close to best ask {best_ask}"
    return True, ""


def get_spread_limit(class_name):
    if class_name == "Class C":
        return 0.08
    if class_name == "Class D":
        return 0.12
    return MAX_SPREAD_PCT


def get_opportunity_score_floor(class_name):
    if class_name == "Class C":
        return MIN_OPPORTUNITY_SCORE_C
    if class_name == "Class D":
        return MIN_OPPORTUNITY_SCORE_D
    return MIN_OPPORTUNITY_SCORE_D


def calculate_density_score(symbol, bid_price, bid_volume, min_density_floor,
                            lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                            buy_vol_5s, sell_vol_5s):
    # 1. Size Score (Size of density relative to floor)
    bid_val_usd = bid_price * bid_volume
    if bid_val_usd >= min_density_floor * 2.0:
        size_score = 4
    elif bid_val_usd >= min_density_floor * 1.5:
        size_score = 3
    else:
        size_score = 2

    # 2. Lifetime Score
    if lifetime_sec < 3.0:
        lifetime_score = 0
    elif lifetime_sec < 10.0:
        lifetime_score = 1
    elif lifetime_sec < 30.0:
        lifetime_score = 2
    elif lifetime_sec < 60.0:
        lifetime_score = 3
    else:
        lifetime_score = 4

    # 3. Refill Score
    if refill_count == 0:
        refill_score = 0
    elif refill_count == 1:
        refill_score = 2
    elif refill_count == 2:
        refill_score = 4
    else:
        refill_score = 6

    # 4. Absorption Score
    if absorption_ratio < 0.5:
        absorption_score = 1
    elif absorption_ratio < 1.0:
        absorption_score = 2
    elif absorption_ratio < 2.0:
        absorption_score = 4
    else:
        absorption_score = 5

    # 5. Delta Score (Tape confirmation)
    if sell_vol_5s > 0:
        delta_ratio = buy_vol_5s / sell_vol_5s
    elif buy_vol_5s > 0:
        delta_ratio = 999.0
    else:
        delta_ratio = 1.0

    if delta_ratio > 1.2:
        delta_score = 3
    else:
        delta_score = 0

    total_score = size_score + lifetime_score + refill_score + absorption_score + delta_score
    return total_score, {
        "size": size_score,
        "lifetime": lifetime_score,
        "refill": refill_score,
        "absorption": absorption_score,
        "delta": delta_score
    }


async def check_and_apply_breakeven(symbol, current_price):
    global positions
    pos = positions.get(symbol)
    if not pos or not isinstance(pos, Position) or not pos.status:
        return
        
    # Check if BE is enabled and not yet activated for this trade
    if not getattr(conf, 'enable_be', 0) == 1:
        return
    if getattr(pos, 'be_activated', False):
        return
        
    entry_price = pos.entry_price
    # Check if the price reached the trigger (+be_trigger_pct%)
    trigger_pct = getattr(conf, 'be_trigger_pct', 0.25)
    trigger_price = entry_price * (1 + trigger_pct / 100)
    
    if current_price >= trigger_price:
        sym_info = all_symbols.get(symbol)
        if not sym_info:
            return
            
        tick_size = sym_info.tick_size
        offset_ticks = getattr(conf, 'be_offset_ticks', 1)
        new_stop_price = round(entry_price + (10 ** -tick_size) * offset_ticks, tick_size)
        
        # Prevent moving stop loss downwards if it was somehow higher
        if pos.stop_price is not None and new_stop_price <= pos.stop_price:
            return
            
        print(f"[Breakeven] Triggered for {symbol}: current {current_price} >= trigger {trigger_price:.6f}. Moving SL: {pos.stop_price} -> {new_stop_price}", flush=True)
        
        # Try updating stop price on Binance
        if not conf.dry_run:
            try:
                # Cancel old SL
                if getattr(pos, 'sl_order_id', None):
                    print(f"[Breakeven] Cancelling old SL order {pos.sl_order_id} on Binance", flush=True)
                    await client.cancel_order(symbol=symbol, orderId=pos.sl_order_id)
                # Place new SL order
                sl_order = await client.new_order(
                    symbol=symbol,
                    side='SELL',
                    type='STOP_MARKET',
                    stopPrice=utils.round_price(new_stop_price, sym_info.tick_size),
                    closePosition='true'
                )
                pos.sl_order_id = sl_order.get('orderId')
                print(f"[Breakeven] Placed new BE SL order on Binance: ID={pos.sl_order_id}", flush=True)
            except Exception as e:
                print(f"[Breakeven ERROR] Failed to adjust SL on Binance for {symbol}: {e}", flush=True)
                # Don't fail the bot, we will retry or just keep tracking
                return

        # Update state in memory
        pos.stop_price = new_stop_price
        pos.be_activated = True
        
        # Update state in DB
        try:
            async with session() as s:
                from sqlalchemy import select, update
                # Find the open trade in DB and update its stop_price
                db_trade = (await s.execute(select(db.Trades).where(db.Trades.symbol == symbol).where(db.Trades.status == 'OPEN').order_by(db.Trades.open_time.desc()).limit(1))).scalar_one_or_none()
                if db_trade:
                    db_trade.stop_price = new_stop_price
                    await s.commit()
                    print(f"[Breakeven] Updated stop_price to {new_stop_price} in DB for {symbol}", flush=True)
        except Exception as e:
            print(f"[Breakeven ERROR] Failed to update stop_price in DB: {e}", flush=True)
            
        # Send TG alert about Breakeven
        try:
            prefix = "🤖 [DRY RUN] " if conf.dry_run else ""
            text = f"{prefix}Переставил Стоп-Лосс в безубыток по #{symbol} на цену <b>{new_stop_price}</b> (цена {current_price})"
            asyncio.create_task(tg.bot.send_message(config.getint('TG', 'CHANNEL'), text))
        except Exception as e:
            print(f"[Breakeven TG Alert Error] {e}", flush=True)


def clear_pending_entry(symbol):
    pending_entry_confirmations.pop(symbol, None)


def get_density_price_ts(density_info):
    if isinstance(density_info, dict):
        return density_info.get("price"), density_info.get("ts", 0)
    if density_info:
        return density_info[0], density_info[1]
    return None, 0


def get_density_value(density_info, key, default=None):
    if isinstance(density_info, dict):
        return density_info.get(key, default)
    return default


async def get_atr_5m(symbol, limit=15):
    try:
        klines = await client.klines(symbol=symbol, interval='5m', limit=limit)
        if len(klines) < 14:
            return None
        tr_sum = 0.0
        for i in range(1, len(klines)):
            high = float(klines[i][2])
            low = float(klines[i][3])
            prev_close = float(klines[i-1][4])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_sum += tr
        return tr_sum / (len(klines) - 1)
    except Exception as e:
        print(f"[ATR] Ошибка получения ATR для {symbol}: {e}")
        return None


async def check_market_delta_anomaly(symbol):
    try:
        # Fetch up to 1000 trades to capture a full 60-second window
        trades = await client.agg_trades(symbol=symbol, limit=1000)
        if not trades:
            return False, 0.0
        
        now_ms = utils.get_ts()
        window_10s = now_ms - 10000
        window_60s = now_ms - 60000
        
        recent_sells = []
        past_sells = []
        
        for t in trades:
            t_time = int(t['T'])
            q = float(t['q'])
            is_buyer_maker = t['m'] # True = Sell market order
            if is_buyer_maker:
                if t_time >= window_10s:
                    recent_sells.append(q)
                elif t_time >= window_60s:
                    past_sells.append(q)
                    
        vol_10s = sum(recent_sells)
        if not past_sells:
            return False, vol_10s
        
        # Calculate adaptive baseline average for a 10-second window based on the actual history length available
        oldest_time = int(trades[0]['T'])
        if oldest_time > window_10s - 2000:
            # Baseline window is less than 2 seconds, not enough history
            return False, vol_10s
            
        if oldest_time > window_60s:
            past_duration_ms = window_10s - oldest_time
            avg_past_10s = sum(past_sells) * (10000.0 / past_duration_ms)
        else:
            avg_past_10s = sum(past_sells) / 5.0
            
        if vol_10s > avg_past_10s * 3.5 and vol_10s > 0:
            print(f"[Delta Filter] Аномальные рыночные продажи по {symbol}: {vol_10s:.2f} > {avg_past_10s * 3.5:.2f} (норма: {avg_past_10s:.2f})")
            return True, vol_10s
        return False, vol_10s
    except Exception as e:
        print(f"[Delta Filter] Ошибка расчета дельты для {symbol}: {e}")
        return False, 0.0


def get_tp_sl_prices(entry_price, sym_info, atr=None):
    # Dynamic ATR exits are deactivated to ensure a stable mathematical expectation;
    # we enforce fixed TP/SL targets (Class C: 0.70%/0.35%, Class D: 0.90%/0.45%) instead.
    atr = None
    symbol = sym_info.symbol
    profile = get_liquidity_class(symbol)
    if profile is None:
        tp_pct = 0.9
        sl_pct = 0.3
    else:
        tp_pct = profile["tp_pct"]
        sl_pct = profile["sl_pct"]
        
    if atr is not None and atr > 0:
        # Волатильный расчет TP/SL на основе ATR-14
        sl_distance = 1.2 * atr
        tp_distance = 3.0 * atr
        
        # Минимальный пол (Floors) - не ближе, чем стандартный sl_pct / tp_pct
        min_stop_distance = entry_price * (sl_pct / 100)
        min_take_distance = entry_price * (tp_pct / 100)
        
        if sl_distance < min_stop_distance:
            sl_distance = min_stop_distance
        if tp_distance < min_take_distance:
            tp_distance = min_take_distance
            
        # Защитное ограничение (Caps) - не дальше, чем 1.5x от стандартного sl_pct / tp_pct
        max_stop_distance = entry_price * (sl_pct * 1.5 / 100)
        max_take_distance = entry_price * (tp_pct * 1.5 / 100)
        
        if sl_distance > max_stop_distance:
            sl_distance = max_stop_distance
        if tp_distance > max_take_distance:
            tp_distance = max_take_distance
            
        take_price = utils.round_up(entry_price + tp_distance, sym_info.tick_size)
        stop_price = utils.round_down(entry_price - sl_distance, sym_info.tick_size)
    else:
        # Стандартный расчет
        take_price = utils.round_up(entry_price * (1 + tp_pct / 100), sym_info.tick_size)
        min_stop_distance = max(10 ** -sym_info.tick_size * conf.stop_loss_ticks, entry_price * (sl_pct / 100))
        # Adaptive cap: stop loss distance should not exceed 1.1 * sl_pct
        max_stop_distance = entry_price * (sl_pct * 1.1 / 100)
        if min_stop_distance > max_stop_distance:
            # Cap the distance, but keep at least 3 ticks of safety buffer
            min_stop_distance = max(10 ** -sym_info.tick_size * 3, max_stop_distance)
        stop_price = utils.round_down(entry_price - min_stop_distance, sym_info.tick_size)
    return take_price, stop_price


def get_price_trend(symbol, current_price):
    """
    Рассчитывает изменение цены в процентах за последние 5 и 15 минут.
    Возвращает (change_5m, change_15m).
    """
    history = price_history.get(symbol, [])
    if not history:
        return 0.0, 0.0
        
    current_time = time.time()
    price_5m = None
    price_15m = None
    
    target_5m = current_time - 300.0
    target_15m = current_time - 900.0
    
    for ts, price in reversed(history):
        if price_5m is None and ts <= target_5m:
            price_5m = price
        if price_15m is None and ts <= target_15m:
            price_15m = price
            break
            
    if price_5m is None and history:
        price_5m = history[0][1]
    if price_15m is None and history:
        price_15m = history[0][1]
        
    change_5m = ((current_price - price_5m) / price_5m * 100) if price_5m else 0.0
    change_15m = ((current_price - price_15m) / price_15m * 100) if price_15m else 0.0
    
    return change_5m, change_15m


# функция для получения цены BNB
async def msg_ticker(ws, msg):
    global bnb_price
    global volumes_24h
    global positions
    global last_prices
    global price_history
    
    # Регуляция вывода дебаг-логов тикера раз в 60 секунд
    global last_ticker_print_time
    if 'last_ticker_print_time' not in globals():
        last_ticker_print_time = 0.0
    import time
    current_time = time.time()
    if current_time - last_ticker_print_time >= 60.0:
        print(f"[DEBUG msg_ticker] Получено {len(msg)} тикеров (лог раз в 60 сек)")
        last_ticker_print_time = current_time
    for sym in msg:
        symbol = sym['s']
        current_price = float(sym['c'])
        last_prices[symbol] = current_price
        if symbol == f"BNB{config['BOT']['ASSET']}":
            # получаем цену BNB
            bnb_price = current_price
        # получаем объемы за 24 часа
        volumes_24h[symbol] = float(sym['q'])

        # Сохраняем цену в историю (не чаще раза в 10 секунд для оптимизации ресурсов)
        if symbol not in price_history:
            price_history[symbol] = []
        history = price_history[symbol]
        if not history or current_time - history[-1][0] >= 10.0:
            history.append((current_time, current_price))
            while len(history) > 1 and current_time - history[0][0] > 900.0:
                history.pop(0)

        # Если включена торговля и режим симуляции (Dry Run), проверяем наши виртуальные позиции
        if conf.trade_mode and conf.dry_run and symbol in positions:
            pos = positions[symbol]
            if isinstance(pos, Position):
                print(f"[DEBUG msg_ticker] Проверка позиции {symbol}: цена {current_price}, pos.status={pos.status}, db_ready={getattr(pos, 'db_ready', False)}, entry={pos.entry_price}")
                if not pos.status:
                    # 1. Проверяем исполнение лимитки на покупку
                    if current_price <= pos.entry_price:
                        # Лимитка исполнена!
                        pos.status = True
                        pos.fill_time = time.time()
                        # Симулируем комиссию (например, 0.02% maker fee)
                        pos.commission = pos.entry_price * pos.quantity * 0.0002
                        pos.qty = pos.quantity
                        print(f"[DRY RUN] Лимитка по {symbol} исполнена по цене {pos.entry_price}")
                        # Выставляем виртуальные тейк и стоп
                        asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity, getattr(pos, 'atr', None)))
                else:
                    # 2. Позиция открыта, проверяем достижение TP или SL
                    # Проверяем, готова ли запись в БД во избежание race condition
                    if getattr(pos, 'db_ready', False):
                        await check_and_apply_breakeven(symbol, current_price)
                        try:
                            sym_info = all_symbols[symbol]
                            take_price = pos.tp_price if pos.tp_price is not None else get_tp_sl_prices(pos.entry_price, sym_info)[0]
                            stop_price = pos.stop_price if pos.stop_price is not None else get_tp_sl_prices(pos.entry_price, sym_info)[1]
                            print(f"[DEBUG msg_ticker] {symbol}: TP={take_price}, SL={stop_price}, current={current_price}")
                        except Exception as e:
                            print(f"[ERROR msg_ticker] Ошибка расчета TP/SL для {symbol}: {e}")
                            continue
                        
                        if current_price >= take_price:
                            # Достигли тейка
                            print(f"[DRY RUN] Достигнут тейк-профит по {symbol} на цене {current_price} (TP={take_price})")
                            # Удаляем из активных позиций сразу же во избежание повторного срабатывания
                            positions.pop(symbol, None)
                            fake_msg = {
                                'o': 'LIMIT',
                                'rp': (take_price - pos.entry_price) * pos.quantity
                            }
                            # Добавляем комиссию на выход (0.02% maker fee)
                            pos.commission += take_price * pos.quantity * 0.0002
                            asyncio.create_task(trade_closed(fake_msg, pos))
                            
                        elif current_price <= stop_price:
                            # Достигли стопа
                            print(f"[DRY RUN] Достигнут стоп-лосс по {symbol} на цене {current_price} (SL={stop_price})")
                            # Удаляем из активных позиций сразу же во избежание повторного срабатывания
                            positions.pop(symbol, None)
                            fake_msg = {
                                'o': 'STOP_MARKET',
                                'rp': (stop_price - pos.entry_price) * pos.quantity
                            }
                            # Добавляем комиссию на выход (0.04% taker fee)
                            pos.commission += stop_price * pos.quantity * 0.0005
                            asyncio.create_task(trade_closed(fake_msg, pos))


# функция для получения плотности из вебсокетов
async def get_density():
    # получаем объект loop (для работы с асинхронными задачами)
    loop = asyncio.get_event_loop()
    # вечный цикл
    while True:
        try:
            # получаем плотность используя run_in_executor (чтобы не блокировать поток выполнения, поскольку
            # метод multiprocessing.Queue.get() является синхронным)
            density = await loop.run_in_executor(None, main_queue.get)
            # передаем плотность в функцию new_density
            await new_density(*density)
        except:
            # ошибки пропускаем
            pass


# функция для асинхронного выхода с задержкой (дебаунс исчезновения плотности)
async def delayed_exit(symbol, pos_obj):
    import time
    if not instant_exits_enabled():
        if positions.get(symbol) is pos_obj:
            pos_obj.pending_exit_task = None
            pos_obj.pending_exit_reason = ''
        return
    await asyncio.sleep(EXIT_DEBOUNCE_SECONDS)
    if positions.get(symbol) is pos_obj and pos_obj.status and pos_obj.pending_exit_task == asyncio.current_task():
        reason = pos_obj.pending_exit_reason or 'density_gone'
        print(f"WebSocket density for {symbol} still unsafe after {EXIT_DEBOUNCE_SECONDS:.1f}s ({reason}). Closing with limit order first.", flush=True)
        pos_obj.pending_exit_task = None
        pos_obj.pending_exit_reason = ''
        await close_position(symbol, limit_first=True)
    return
    await asyncio.sleep(2.0)
    # Сверяем, что в памяти все еще эта же позиция, ее статус активен и задание не отменено
    if positions.get(symbol) is pos_obj and pos_obj.status and pos_obj.pending_exit_task == asyncio.current_task():
        print(f"WebSocket плотность по {symbol} не вернулась за 2 секунды. Выходим по лимитке.", flush=True)
        pos_obj.pending_exit_task = None
        await close_position(symbol, limit_first=True)

# функция для обработки плотности
async def new_density(symbol, bid_price, bid_volume, ask_volume, num, sent_ns=None, event_time=None, best_bid=None, best_ask=None,
                      lifetime_sec=0.0, refill_count=0, refill_ratio=0.0, absorption_ratio=0.0,
                      buy_vol_5s=0.0, sell_vol_5s=0.0, opportunity_score=0.0, book_imbalance=0.0,
                      microprice_edge_pct=0.0, spread_pct_ob=0.0, distance_pct=0.0,
                      density_ratio=0.0, wall_to_ask_ratio=0.0, delta_ratio=1.0):
    global positions
    global volumes_24h
    global last_prices
    try:
        import time
        if best_bid is not None:
            last_prices[symbol] = best_bid

        if sent_ns is not None:
            ipc_delay = (time.perf_counter_ns() - sent_ns) / 1_000_000
            net_delay = (int(time.time() * 1000) - event_time) if event_time else 0
            if bid_price is not None:
                print(f"[Latency] {symbol}: Network = {net_delay}ms | IPC Queue = {ipc_delay:.2f}ms | Total = {net_delay + ipc_delay:.2f}ms")

        # записываем плотность в словарь (для отслеживания необходимости отмены лимиток, если цена ушла далеко от них)
        if bid_price is not None:
            symbols_densities[symbol] = {
                "price": bid_price,
                "ts": utils.get_ts(),
                "volume": bid_volume,
                "value_usd": bid_price * bid_volume,
                "ask_volume": ask_volume,
                "level": num,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "lifetime_sec": lifetime_sec,
                "refill_count": refill_count,
                "refill_ratio": refill_ratio,
                "absorption_ratio": absorption_ratio,
                "buy_vol_5s": buy_vol_5s,
                "sell_vol_5s": sell_vol_5s,
                "opportunity_score": opportunity_score,
                "book_imbalance": book_imbalance,
                "microprice_edge_pct": microprice_edge_pct,
                "spread_pct": spread_pct_ob,
                "distance_pct": distance_pct,
                "density_ratio": density_ratio,
                "wall_to_ask_ratio": wall_to_ask_ratio,
                "delta_ratio": delta_ratio
            }
        else:
            symbols_densities.pop(symbol, None)
            clear_pending_entry(symbol)

        # 1. ОБРАБОТКА МГНОВЕННОЙ ОТМЕНЫ (Плотность исчезла)
        if bid_price is None:
            if symbol in positions:
                pos = positions[symbol]
                if isinstance(pos, Position):
                    if not pos.status:
                        if conf.dry_run:
                            current_price = last_prices.get(symbol)
                            if current_price and current_price <= pos.entry_price:
                                # Лимитка исполнена!
                                pos.status = True
                                pos.fill_time = time.time()
                                pos.commission = pos.entry_price * pos.quantity * 0.0002
                                pos.qty = pos.quantity
                                print(f"[DRY RUN] Лимитка по {symbol} исполнена по цене {pos.entry_price} (при отмене плотности)")
                                asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity, getattr(pos, 'atr', None)))
                                return
                        print(f"WebSocket мгновенная отмена лимитки на вход {symbol} цена {pos.entry_price}")
                        asyncio.create_task(cancel_limit(symbol, pos.order_id))
                    else:
                        if not instant_exits_enabled():
                            if pos.pending_exit_task:
                                pos.pending_exit_task.cancel()
                                pos.pending_exit_task = None
                            pos.pending_exit_reason = ''
                            print(f"[Instant Exit disabled] {symbol}: density disappeared, keeping position until TP/SL.", flush=True)
                            return
                        # Если цена уже пробила уровень стопа — выходим НЕМЕДЛЕННО, минуя дебаунс
                        _cur_price = last_prices.get(symbol)
                        _sym_info = all_symbols.get(symbol)
                        if _cur_price and _sym_info and pos.entry_price > 0:
                            try:
                                _stop_px = pos.stop_price if pos.stop_price is not None else get_tp_sl_prices(pos.entry_price, _sym_info)[1]
                                if _cur_price <= _stop_px:
                                    print(f"[Стоп-пробой] {symbol}: цена {_cur_price} <= стоп {_stop_px}. Закрываем НЕМЕДЛЕННО!", flush=True)
                                    if pos.pending_exit_task:
                                        pos.pending_exit_task.cancel()
                                        pos.pending_exit_task = None
                                    pos.pending_exit_reason = 'stop_breach_on_density_gone'
                                    asyncio.create_task(close_position(symbol))
                                    return
                            except Exception:
                                pass
                        # ДЕБАУНС: планируем выход через 2 секунды вместо мгновенного закрытия
                        if not pos.pending_exit_task or pos.pending_exit_task.done():
                            pos.pending_exit_reason = 'density_gone'
                            print(f"WebSocket плотность по {symbol} исчезла! Планируем выход по рынку с дебаунсом 2 секунды...", flush=True)
                            pos.pending_exit_task = asyncio.create_task(delayed_exit(symbol, pos))
                elif pos is True:
                    print(f"WebSocket плотность исчезла во время выставления ордера {symbol}. Помечаем на отмену.")
                    positions[symbol] = "CANCEL_PENDING"
                    clear_pending_entry(symbol)
            return

        # если торговля отключена
        if not conf.trade_mode:
            return

        # 2. ОБРАБОТКА СМЕЩЕНИЯ ПЛОТНОСТИ (Плотность переместилась)
        if symbol in positions:
            pos = positions[symbol]
            if isinstance(pos, Position):
                # Если у нас запланирован выход по рынку, но плотность вернулась на той же или лучшей цене, отменяем выход
                if pos.pending_exit_task and not pos.pending_exit_task.done() and bid_price >= pos.density:
                    print(f"WebSocket плотность по {symbol} вернулась на цене {bid_price}! Отменяем дебаунс выхода.", flush=True)
                    pos.pending_exit_task.cancel()
                    pos.pending_exit_task = None
                    pos.pending_exit_reason = ''
                
                # Если ордер еще не исполнен, а плотность сместилась — отменяем старый ордер
                if not pos.status and pos.density != bid_price:
                    if conf.dry_run:
                        current_price = last_prices.get(symbol)
                        if current_price and current_price <= pos.entry_price:
                            # Лимитка исполнена!
                            pos.status = True
                            pos.fill_time = time.time()
                            pos.commission = pos.entry_price * pos.quantity * 0.0002
                            pos.qty = pos.quantity
                            print(f"[DRY RUN] Лимитка по {symbol} исполнена по цене {pos.entry_price} (при смещении плотности)")
                            asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity, getattr(pos, 'atr', None)))
                            return
                    print(f"Плотность по {symbol} сместилась ({pos.density} -> {bid_price}). Мгновенно переставляем ордер.")
                    asyncio.create_task(cancel_limit(symbol, pos.order_id))
                    positions.pop(symbol, None)
                    # Даем провалиться дальше, чтобы выставить новую лимитку на следующем шаге
                elif pos.status and bid_price == pos.density:
                    # Детекция разъедания плотности (если объем упал ниже DENSITY_EATEN_THRESHOLD_PCT)
                    if instant_exits_enabled():
                        if pos.density_volume > 0 and bid_volume < pos.density_volume * DENSITY_EATEN_THRESHOLD_PCT:
                            if time.time() - pos.fill_time >= MIN_POSITION_HOLD_SECONDS:
                                if not pos.pending_exit_task or pos.pending_exit_task.done():
                                    pos.pending_exit_reason = 'density_eaten'
                                    print(f"WebSocket плотность по {symbol} разъедена ({bid_volume:.2f} < {pos.density_volume * DENSITY_EATEN_THRESHOLD_PCT:.2f}). Планируем выход по рынку с дебаунсом.", flush=True)
                                    pos.pending_exit_task = asyncio.create_task(delayed_exit(symbol, pos))
                    # Если плотность на том же уровне и объем в порядке, отменяем запланированный выход
                    elif pos.pending_exit_task and not pos.pending_exit_task.done():
                        print(f"WebSocket плотность по {symbol} восстановлена/сохранена! Отменяем дебаунс выхода.", flush=True)
                        pos.pending_exit_task.cancel()
                        pos.pending_exit_task = None
                        pos.pending_exit_reason = ''
                elif pos.status and bid_price < pos.density:
                    if not instant_exits_enabled():
                        if pos.pending_exit_task:
                            pos.pending_exit_task.cancel()
                            pos.pending_exit_task = None
                        pos.pending_exit_reason = ''
                        print(f"[Instant Exit disabled] {symbol}: density moved down ({pos.density} -> {bid_price}), keeping position until TP/SL.", flush=True)
                        return
                    current_price = last_prices.get(symbol, best_bid or bid_price)
                    _sym_info = all_symbols.get(symbol)
                    if current_price and _sym_info and pos.entry_price > 0:
                        try:
                            _stop_px = pos.stop_price if pos.stop_price is not None else get_tp_sl_prices(pos.entry_price, _sym_info)[1]
                            if current_price <= _stop_px:
                                print(f"[Stop breach] {symbol}: price {current_price} <= stop {_stop_px}. Closing immediately.", flush=True)
                                if pos.pending_exit_task:
                                    pos.pending_exit_task.cancel()
                                    pos.pending_exit_task = None
                                pos.pending_exit_reason = 'stop_breach_on_density_shift'
                                asyncio.create_task(close_position(symbol))
                                return
                        except Exception:
                            pass
                    if not pos.pending_exit_task or pos.pending_exit_task.done():
                        pos.pending_exit_reason = 'density_shift_down'
                        print(f"WebSocket density for {symbol} moved down ({pos.density} -> {bid_price}). Scheduling market exit with debounce.", flush=True)
                        pos.pending_exit_task = asyncio.create_task(delayed_exit(symbol, pos))
                    return
                    # Если плотность сместилась вниз, выходим немедленно, отменяя запланированный дебаунс если он был
                    if pos.pending_exit_task:
                        pos.pending_exit_task.cancel()
                        pos.pending_exit_task = None
                    print(f"WebSocket плотность по {symbol} сместилась вниз ({pos.density} -> {bid_price})! Выходим по рынку для защиты.")
                    asyncio.create_task(close_position(symbol))
                    return
                elif pos.status and bid_price > pos.density:
                    # Если плотность сместилась вверх, обновляем ее значение для подтягивания защиты
                    print(f"Плотность по {symbol} сместилась вверх ({pos.density} -> {bid_price}). Обновляем защитный уровень.")
                    pos.density = bid_price
                else:
                    # Если ордер уже в рынке или цена та же — ничего не делаем
                    return
            else:
                # Если pos == True или "CANCEL_PENDING" (ордер в процессе выставления) — пропускаем
                return

        # 3. ФИЛЬТРЫ ЛИКВИДНОСТИ (ADV) И ГЛУБИНЫ СТАКАНА (LEVEL)
        # Исключаем уровень 0 (лучший bid), чтобы избежать немедленного taker-исполнения и входа на ложных плотностях
        if num < 1 or num > conf.min_density_num:
            if num < 1:
                print(f"[Фильтр стакана] Пропускаем {symbol}: плотность на лучшем bid (level 0), высокий риск taker-проскальзывания")
            else:
                print(f"[Фильтр стакана] Пропускаем {symbol}: плотность слишком глубоко (level {num} > {conf.min_density_num})")
            return

        # Получаем 24-часовой объем торгов
        profile = get_liquidity_class(symbol)
        if profile is None:
            print(f"[Liquidity filter] Skip {symbol}: 24h volume is not loaded yet.")
            return
        if profile["blocked"]:
            print(f"[Liquidity filter] Skip {symbol}: {profile['name']} (24h vol: ${round(volumes_24h.get(symbol, 0.0), 2)})")
            return
        vol_24h = volumes_24h.get(symbol, 0.0)
        
        min_density_floor, class_name = get_min_density_floor(symbol, bid_price)
        if min_density_floor is None:
            print(f"[Фильтр ликвидности] Пропускаем {symbol}: {class_name} не подходит для density-bounce")
            return

        bid_val_usd = bid_price * bid_volume
        if bid_val_usd < min_density_floor:
            print(f"[Фильтр ликвидности] Пропускаем {symbol}: {class_name} (объем плотности ${round(bid_val_usd, 2)} ниже порога ${round(min_density_floor, 0)})")
            return

        if balance < conf.order_size * config.getfloat('BOT', 'MIN_NOTIONAL'):
            print(f"{symbol}: {balance} < {conf.order_size} недостаточно средств")
            return
        if symbol in conf.blacklist:
            print(f"{symbol} в черном списке")
            return
            
        # Глобальный фильтр тренда BTC/ETH
        btc_price = last_prices.get('BTCUSDT')
        if btc_price:
            btc_5m, btc_15m = get_price_trend('BTCUSDT', btc_price)
            if btc_5m < BTC_MAX_DROP_5M_PCT or btc_15m < BTC_MAX_DROP_15M_PCT:
                print(f"[BTC фильтр] Пропускаем {symbol}: BTC падает (5m: {btc_5m:+.2f}%, 15m: {btc_15m:+.2f}%)")
                return

        eth_price = last_prices.get('ETHUSDT')
        if eth_price:
            eth_5m, eth_15m = get_price_trend('ETHUSDT', eth_price)
            if eth_5m < ETH_MAX_DROP_5M_PCT or eth_15m < ETH_MAX_DROP_15M_PCT:
                print(f"[ETH фильтр] Пропускаем {symbol}: ETH падает (5m: {eth_5m:+.2f}%, 15m: {eth_15m:+.2f}%)")
                return

        # 2.5.5. ЛОКАЛЬНЫЙ ТРЕНДОВЫЙ ФИЛЬТР (Защита от входов против тренда)
        current_price = last_prices.get(symbol, bid_price)
        change_5m, change_15m = get_price_trend(symbol, current_price)
        if change_5m < MAX_DROP_5M_PCT or change_15m < MAX_DROP_15M_PCT:
            print(f"[Трендовый фильтр] Пропускаем {symbol}: монета в даунтренде (5m: {change_5m:+.2f}%, 15m: {change_15m:+.2f}%)")
            return

        # 2.5.6. ФИЛЬТР СПРЕДА (Защита от проскальзывания при широком спреде)
        if best_bid is not None and best_ask is not None:
            spread_pct = (best_ask - best_bid) / best_bid * 100
            max_spread = get_spread_limit(class_name)
            if spread_pct > max_spread:
                print(f"[Фильтр спреда] Пропускаем {symbol}: спред слишком широкий ({spread_pct:.3f}% > {max_spread:.2f}%)")
                return
        # если символ дал WR < 20% за последние 10 сделок -> пауза 30 мин
        if symbol in symbol_recent_trades:
            recent = symbol_recent_trades[symbol]
            if len(recent) >= 10:
                wins = sum(1 for _, is_win in recent if is_win)
                wr = (wins / len(recent)) * 100
                if wr < 20.0:
                    last_close_time = recent[-1][0]
                    elapsed_min = (utils.get_ts() - last_close_time) / 60000.0
                    if elapsed_min < 30.0:
                        print(f"[WR Фильтр] Пропускаем {symbol}: WR за последние 10 сделок = {wr:.1f}% (< 20%), пауза {30.0 - elapsed_min:.1f} мин.")
                        return
        # Фильтр максимальной дистанции до плотности (не более 0.6%)
        current_price = last_prices.get(symbol, bid_price)
        if current_price > bid_price * 1.006:
            # Пропускаем без логирования во избежание спама для слишком глубоких плит
            return
        if distance_pct > MAX_ENTRY_DISTANCE_PCT:
            print(f"[Alpha filter] Skip {symbol}: density too deep ({distance_pct:.3f}% > {MAX_ENTRY_DISTANCE_PCT:.2f}%)")
            return
        min_ob_score = get_opportunity_score_floor(class_name)
        if opportunity_score < min_ob_score:
            print(f"[Alpha filter] Skip {symbol}: opportunity_score {opportunity_score:.2f} < {min_ob_score:.2f} "
                  f"(imb={book_imbalance:.2f}, micro={microprice_edge_pct:.4f}%, dist={distance_pct:.3f}%, delta={delta_ratio:.2f})")
            return

        # если позиция по этой паре уже открыта, то пропускаем
        if symbol in positions:
            print(f"{symbol} {bid_price} уже открыта")
            return
        # если уже открыто слишком много позиций, то пропускаем (считаем только реальные Position)
        active_positions_count = sum(1 for p in positions.values() if isinstance(p, Position))
        if active_positions_count >= conf.max_positions:
            print(f"{symbol} {bid_price} уже открыто слишком много позиций (активных: {active_positions_count} >= {conf.max_positions})")
            return
        # проверка объема за 24 часа
        if symbol in volumes_24h:
            # если объем меньше min_volume_24h, то пропускаем
            if volumes_24h[symbol] < conf.min_volume_24h:
                print(f"{symbol} {bid_price} {volumes_24h[symbol]} слишком маленький объем за 24 часа")
                return
            # если объем больше max_volume_24h, то пропускаем
            elif volumes_24h[symbol] > conf.max_volume_24h:
                print(f"{symbol} {bid_price} {volumes_24h[symbol]} слишком большой объем за 24 часа")
                return
        # вносим позицию в словарь, чтобы у нас не открылась позиция несколько раз на следующем тике
        sym_info = all_symbols.get(symbol)
        if not sym_info:
            print(f"[Entry filter] Skip {symbol}: symbol info is not loaded.")
            return
        expected_entry = round(bid_price + 10 ** -sym_info.tick_size, sym_info.tick_size)
        maker_ok, maker_reason = is_maker_entry_safe(expected_entry, best_ask, 10 ** -sym_info.tick_size)
        if not maker_ok:
            print(f"[Maker guard] Skip {symbol}: {maker_reason}")
            return
        expected_take, expected_stop = get_tp_sl_prices(expected_entry, sym_info)
        rr_ok, rr = is_reward_risk_ok(expected_entry, expected_take, expected_stop)
        if not rr_ok:
            print(f"[RR filter] Skip {symbol}: reward/risk {rr:.2f} < {MIN_REWARD_RISK_RATIO:.2f} (entry={expected_entry}, TP={expected_take}, SL={expected_stop})")
            return
        net_ok, net_rr, net_reward_pct, net_risk_pct = estimate_net_reward_risk(expected_entry, expected_take, expected_stop)
        if not net_ok:
            print(f"[Net RR filter] Skip {symbol}: net reward/risk {net_rr:.2f} < {MIN_NET_REWARD_RISK_RATIO:.2f} "
                  f"(net +{net_reward_pct:.3f}% / -{net_risk_pct:.3f}%)")
            return
        pending_entry_confirmations[symbol] = {
            "density_price": bid_price,
            "bid_volume": bid_volume,
            "bid_val_usd": bid_val_usd,
            "min_density_floor": min_density_floor,
            "class_name": class_name,
            "created_ts": utils.get_ts(),
            "level": num,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "entry_price": expected_entry,
            "take_price": expected_take,
            "stop_price": expected_stop,
            "rr": rr,
            "net_rr": net_rr,
            "net_reward_pct": net_reward_pct,
            "net_risk_pct": net_risk_pct,
            "opportunity_score": opportunity_score,
            "book_imbalance": book_imbalance,
            "microprice_edge_pct": microprice_edge_pct,
            "spread_pct": spread_pct_ob,
            "distance_pct": distance_pct,
            "density_ratio": density_ratio,
            "wall_to_ask_ratio": wall_to_ask_ratio,
            "delta_ratio": delta_ratio,
        }
        positions[symbol] = True
        # запускаем задачу открытия позиции
        asyncio.create_task(open_position_delayed(symbol, bid_price))
    except:
        # выводим ошибку
        print(f"Ошибка в функции new_density: {traceback.format_exc()}")


# Entry Confirmation: ждём 3 секунды и убеждаемся, что плотность ещё жива
async def open_position_delayed(symbol, bid_price):
    await asyncio.sleep(ENTRY_CONFIRMATION_DELAY)

    def reject(reason):
        print(f"[Entry Confirm] {symbol}: {reason}. Entry cancelled.", flush=True)
        positions.pop(symbol, None)
        clear_pending_entry(symbol)

    candidate = pending_entry_confirmations.get(symbol)
    current_state = positions.get(symbol)
    if current_state is not True:
        if current_state == "CANCEL_PENDING":
            positions.pop(symbol, None)
        clear_pending_entry(symbol)
        print(f"[Entry Confirm] {symbol}: state changed during confirmation wait. Entry skipped.", flush=True)
        return
    if not candidate:
        reject("no original density snapshot")
        return

    density_info = symbols_densities.get(symbol)
    live_density_price, _density_ts = get_density_price_ts(density_info)
    if live_density_price is None:
        reject("original density disappeared")
        return

    sym_info = all_symbols.get(symbol)
    if not sym_info:
        reject("symbol info is not loaded")
        return
    tick = 10 ** -sym_info.tick_size
    if abs(live_density_price - candidate["density_price"]) > tick / 2:
        reject(f"density moved from {candidate['density_price']} to {live_density_price}")
        return

    live_bid_volume = get_density_value(density_info, "volume", candidate["bid_volume"])
    live_value_usd = live_density_price * live_bid_volume
    
    # 1. КОНТРОЛЬ ОСТАТОЧНОГО ОБЪЕМА (Снижение объема не более чем на 20%)
    if live_bid_volume < candidate["bid_volume"] * 0.8:
        reject(f"density volume dropped {live_bid_volume:.6f} < {candidate['bid_volume'] * 0.8:.6f} (eaten/canceled)")
        return
    if live_value_usd < candidate["min_density_floor"]:
        reject(f"density value ${live_value_usd:.2f} below floor ${candidate['min_density_floor']:.2f}")
        return

    live_best_bid = get_density_value(density_info, "best_bid", candidate.get("best_bid"))
    live_best_ask = get_density_value(density_info, "best_ask", candidate.get("best_ask"))
    if live_best_bid and live_best_ask:
        spread_pct = (live_best_ask - live_best_bid) / live_best_bid * 100
        class_name = candidate.get("class_name", "Class C")
        max_spread = get_spread_limit(class_name)
        if spread_pct > max_spread:
            reject(f"spread widened to {spread_pct:.3f}% (max: {max_spread:.2f}%)")
            return
    else:
        class_name = candidate.get("class_name", "Class C")

    live_distance_pct = get_density_value(density_info, "distance_pct", candidate.get("distance_pct", 0.0))
    if live_distance_pct > MAX_ENTRY_DISTANCE_PCT:
        reject(f"density too deep after wait: {live_distance_pct:.3f}% > {MAX_ENTRY_DISTANCE_PCT:.2f}%")
        return

    live_opportunity_score = get_density_value(density_info, "opportunity_score", candidate.get("opportunity_score", 0.0))
    min_ob_score = get_opportunity_score_floor(class_name)
    if live_opportunity_score < min_ob_score:
        reject(f"opportunity score decayed: {live_opportunity_score:.2f} < {min_ob_score:.2f}")
        return

    # 2. ПРОВЕРКА БЛИЗОСТИ ЦЕНЫ (Защита от спуфинга: цена должна быть близко к плотности, в пределах 0.50%)
    current_price = last_prices.get(symbol, live_best_bid or live_density_price)
    if current_price > live_density_price * 1.0050:
        reject(f"price is too far from density: {current_price} > {live_density_price * 1.0050:.6f} (potential spoofing)")
        return

    # 3. ФИЛЬТР АНОМАЛЬНОГО УСКОРЕНИЯ ПРОДАЖ (Delta Z-Score)
    is_delta_anomaly, sell_vol = await check_market_delta_anomaly(symbol)
    if is_delta_anomaly:
        reject(f"market panic detected: sell volume spike {sell_vol:.2f} (falling knife)")
        return

    # 4. ПОЛУЧЕНИЕ ATR-14 И АДАПТИВНЫХ TP/SL
    atr = await get_atr_5m(symbol)
    if atr is not None:
        print(f"[ATR Confirm] {symbol}: calculated 5m ATR={atr:.6f} ({atr/current_price*100:.3f}%)")

    # Проверка глобального тренда BTC/ETH во время подтверждения входа
    btc_price = last_prices.get('BTCUSDT')
    if btc_price:
        btc_5m, btc_15m = get_price_trend('BTCUSDT', btc_price)
        if btc_5m < BTC_MAX_DROP_5M_PCT or btc_15m < BTC_MAX_DROP_15M_PCT:
            reject(f"BTC downtrend during confirmation (5m: {btc_5m:+.2f}%, 15m: {btc_15m:+.2f}%)")
            return

    eth_price = last_prices.get('ETHUSDT')
    if eth_price:
        eth_5m, eth_15m = get_price_trend('ETHUSDT', eth_price)
        if eth_5m < ETH_MAX_DROP_5M_PCT or eth_15m < ETH_MAX_DROP_15M_PCT:
            reject(f"ETH downtrend during confirmation (5m: {eth_5m:+.2f}%, 15m: {eth_15m:+.2f}%)")
            return

    change_5m, change_15m = get_price_trend(symbol, current_price)
    if change_5m < MAX_DROP_5M_PCT or change_15m < MAX_DROP_15M_PCT:
        reject(f"downtrend after wait (5m: {change_5m:+.2f}%, 15m: {change_15m:+.2f}%)")
        return

    entry_price = round(live_density_price + tick, sym_info.tick_size)
    maker_ok, maker_reason = is_maker_entry_safe(entry_price, live_best_ask, tick)
    if not maker_ok:
        reject(f"maker guard failed: {maker_reason}")
        return
    take_price, stop_price = get_tp_sl_prices(entry_price, sym_info, atr)
    rr_ok, rr = is_reward_risk_ok(entry_price, take_price, stop_price)
    if not rr_ok:
        reject(f"reward/risk {rr:.2f} < {MIN_REWARD_RISK_RATIO:.2f}")
        return
    net_ok, net_rr, net_reward_pct, net_risk_pct = estimate_net_reward_risk(entry_price, take_price, stop_price)
    if not net_ok:
        reject(f"net reward/risk {net_rr:.2f} < {MIN_NET_REWARD_RISK_RATIO:.2f} "
               f"(net +{net_reward_pct:.3f}% / -{net_risk_pct:.3f}%)")
        return

    # 5. РАСЧЕТ И ПРОВЕРКА DENSITY SCORE (v9)
    lifetime_sec = density_info.get("lifetime_sec", 0.0)
    refill_count = density_info.get("refill_count", 0)
    refill_ratio = density_info.get("refill_ratio", 0.0)
    absorption_ratio = density_info.get("absorption_ratio", 0.0)
    buy_vol_5s = density_info.get("buy_vol_5s", 0.0)
    sell_vol_5s = density_info.get("sell_vol_5s", 0.0)

    score, breakdown = calculate_density_score(
        symbol, live_density_price, live_bid_volume, candidate["min_density_floor"],
        lifetime_sec, refill_count, refill_ratio, absorption_ratio, buy_vol_5s, sell_vol_5s
    )

    # Dynamic score threshold based on liquidity class: C=6 (active), D=10 (protective floor for thin coins)
    score_threshold = 6 if class_name == "Class C" else 10 if class_name == "Class D" else getattr(conf, "score_threshold", 12)
    if score < score_threshold:
        reject(f"low density score: {score} < {score_threshold} (Breakdown: {breakdown}) (Class: {class_name})")
        return

    alpha_score = score + live_opportunity_score
    print(f"[Score Confirm] {symbol}: confirmed score={score}, ob_score={live_opportunity_score:.2f}, "
          f"alpha={alpha_score:.2f}, netRR={net_rr:.2f} "
          f"(Size={breakdown['size']}, Life={breakdown['lifetime']}, Refill={breakdown['refill']}, "
          f"Abs={breakdown['absorption']}, Delta={breakdown['delta']})", flush=True)

    candidate.update({
        "density_price": live_density_price,
        "bid_volume": live_bid_volume,
        "bid_val_usd": live_value_usd,
        "entry_price": entry_price,
        "take_price": take_price,
        "stop_price": stop_price,
        "rr": rr,
        "net_rr": net_rr,
        "net_reward_pct": net_reward_pct,
        "net_risk_pct": net_risk_pct,
        "atr": atr,
        "opportunity_score": live_opportunity_score,
        "alpha_score": alpha_score,
        "distance_pct": live_distance_pct,
        "book_imbalance": get_density_value(density_info, "book_imbalance", candidate.get("book_imbalance", 0.0)),
        "microprice_edge_pct": get_density_value(density_info, "microprice_edge_pct", candidate.get("microprice_edge_pct", 0.0)),
        "delta_ratio": get_density_value(density_info, "delta_ratio", candidate.get("delta_ratio", 1.0)),
    })
    print(f"[Entry Confirm] {symbol}: density confirmed after {ENTRY_CONFIRMATION_DELAY:.0f}s, "
          f"RR={rr:.2f}, netRR={net_rr:.2f}. Entering near {live_density_price}.", flush=True)
    await open_position(symbol, live_density_price, atr)
    return


# функция для открытия позиции
async def open_position(symbol, bid_price, atr=None):
    global positions
    print(f"Открытие позиции по {symbol} цена {bid_price}")
    try:
        # Double check position limit before placing order
        active_positions_count = sum(1 for p in positions.values() if isinstance(p, Position))
        if active_positions_count >= conf.max_positions:
            print(f"[Entry] {symbol}: cancel opening. Too many active positions already: {active_positions_count} >= {conf.max_positions}")
            positions.pop(symbol, None)
            clear_pending_entry(symbol)
            return

        # получаем информацию о паре
        sym_info = all_symbols.get(symbol)
        if not sym_info:
            print(f"[Entry filter] Skip {symbol}: symbol info is not loaded.")
            positions.pop(symbol, None)
            clear_pending_entry(symbol)
            return
        pending_snapshot = pending_entry_confirmations.get(symbol, {})
        profile = get_liquidity_class(symbol)
        if profile is None or profile["blocked"]:
            print(f"[Entry filter] Skip {symbol}: liquidity profile is unavailable or blocked.")
            positions.pop(symbol, None)
            clear_pending_entry(symbol)
            return
        # расчитываем цену входа (нам нужно выставить цену лимитки большую на 1 шаг цены)
        entry_price = round(bid_price + 10 ** -sym_info.tick_size, sym_info.tick_size)
        live_density_info = symbols_densities.get(symbol, {})
        live_best_ask = get_density_value(live_density_info, "best_ask", pending_snapshot.get("best_ask"))
        maker_ok, maker_reason = is_maker_entry_safe(entry_price, live_best_ask, 10 ** -sym_info.tick_size)
        if not maker_ok:
            print(f"[Maker guard] Skip {symbol} at order placement: {maker_reason}")
            positions.pop(symbol, None)
            clear_pending_entry(symbol)
            return
        take_price, stop_price = get_tp_sl_prices(entry_price, sym_info, atr)
        rr_ok, rr = is_reward_risk_ok(entry_price, take_price, stop_price)
        if not rr_ok:
            print(f"[RR filter] Skip {symbol}: reward/risk {rr:.2f} < {MIN_REWARD_RISK_RATIO:.2f} at order placement.")
            positions.pop(symbol, None)
            clear_pending_entry(symbol)
            return
        net_ok, net_rr, net_reward_pct, net_risk_pct = estimate_net_reward_risk(entry_price, take_price, stop_price)
        if not net_ok:
            print(f"[Net RR filter] Skip {symbol}: net reward/risk {net_rr:.2f} < {MIN_NET_REWARD_RISK_RATIO:.2f} at order placement "
                  f"(net +{net_reward_pct:.3f}% / -{net_risk_pct:.3f}%).")
            positions.pop(symbol, None)
            clear_pending_entry(symbol)
            return
        density_volume = pending_snapshot.get("bid_volume", 0.0)
        # расчитываем объем входа на основе order_size
        quantity = utils.round_down(conf.order_size / entry_price, sym_info.step_size)
        # если цена входа меньше min_notional, то увеличиваем объем на min_notional с запасом
        min_notional = getattr(sym_info, 'min_notional', getattr(sym_info, 'notional', 5.0))
        if quantity * entry_price < min_notional * config.getfloat('BOT', 'MIN_NOTIONAL'):
            quantity = utils.round_up(min_notional * config.getfloat('BOT', 'MIN_NOTIONAL') / entry_price, sym_info.step_size)
        
        # выставляем лимитку
        if conf.dry_run:
            # В режиме симуляции генерируем уникальный отрицательный ID ордера
            order_id = -int(utils.get_ts() % 10000000)
            
            # Проверяем, не пришел ли сигнал отмены во время расчета
            if positions.get(symbol) == "CANCEL_PENDING":
                print(f"[DRY RUN] Плотность по {symbol} исчезла до выставления ордера. Отмена.")
                positions.pop(symbol, None)
                return
                
            pos = Position(symbol, quantity, entry_price, bid_price, density_volume, atr)
            pos.order_id = order_id
            pos.tp_price = take_price
            pos.stop_price = stop_price
            positions[symbol] = pos
            
            print(f"[DRY RUN] Выставил симуляционный лимит по {symbol} цена {entry_price} объем {quantity}")
        else:
            try:
                import time
                try:
                    await client.change_leverage(symbol=symbol, leverage=int(conf.leverage))
                except Exception as le:
                    print(f"Не удалось установить плечо {conf.leverage} для {symbol}: {le}")
                t0 = time.perf_counter()
                order = await client.new_order(symbol=symbol, side='BUY', type='LIMIT',
                                               timeInForce='GTX',
                                               price=utils.round_price(entry_price, sym_info.tick_size),
                                               quantity=quantity)
                order_id = order['orderId']
                api_latency = (time.perf_counter() - t0) * 1000
                print(f"[Latency] {symbol}: Binance API Order placement took {api_latency:.1f}ms")
                
                # Проверяем, не пришел ли сигнал отмены во время отправки запроса
                if positions.get(symbol) == "CANCEL_PENDING":
                    print(f"Плотность по {symbol} исчезла во время выставления ордера. Отменяем ордер {order_id} на бирже.")
                    try:
                        await client.cancel_order(symbol=symbol, orderId=order_id)
                        positions.pop(symbol, None)
                    except binance.error.ClientException as e:
                        if e.error_code == -2011:
                            print(f"Не удалось отменить ордер {order_id} по {symbol}: ордер уже исполнен (Error -2011). Сохраняем позицию для отслеживания.")
                            pos = Position(symbol, quantity, entry_price, bid_price, density_volume, atr)
                            pos.order_id = order_id
                            pos.tp_price = take_price
                            pos.stop_price = stop_price
                            positions[symbol] = pos
                        else:
                            positions.pop(symbol, None)
                    except Exception as e:
                        print(f"Ошибка при попытке отмены ордера {order_id} по {symbol}: {e}")
                        positions.pop(symbol, None)
                    return
                
                pos = Position(symbol, quantity, entry_price, bid_price, density_volume, atr)
                pos.order_id = order_id
                pos.tp_price = take_price
                pos.stop_price = stop_price
                positions[symbol] = pos
                print(f"Выставил лимитку по {symbol} цена {entry_price} объем {quantity}")
            except Exception as e:
                # если не удалось выставить лимитку, то удаляем позицию
                positions.pop(symbol, None)
                print(f"Не удалось выставить лимитку по {symbol} цена {entry_price}: {e}")
                return
    except Exception as e:
        # если не удалось открыть позицию, то удаляем позицию
        positions.pop(symbol, None)
        print(f"Не удалось открыть позицию по {symbol} цена {bid_price}\n{traceback.format_exc()}")
        return


# функция для обработки сообщений вебсокета UserData
async def msg_userdata(ws, msg):
    global positions
    global balance
    # определяем тип сообщения
    event_type = msg.get('e')
    match event_type:
        # исполнение ордера на фьючерсах
        case 'ORDER_TRADE_UPDATE':
            o = msg.get('o', {})
            status = o.get('X')
            side = o.get('S')
            order_type = o.get('o')
            symbol = o.get('s')
            if status in ('FILLED', 'PARTIALLY_FILLED'):
                state = positions.get(symbol)
                if state and not isinstance(state, Position):
                    if side == 'BUY' and order_type == 'LIMIT':
                        candidate = pending_entry_confirmations.get(symbol, {})
                        avg_price = float(o.get('ap') or o.get('L') or o.get('p') or candidate.get("entry_price", 0) or 0)
                        qty = float(o.get('z') or o.get('q') or 0)
                        if avg_price > 0 and qty > 0:
                            pos = Position(
                                symbol,
                                qty,
                                avg_price,
                                candidate.get("density_price", avg_price),
                                candidate.get("bid_volume", 0.0),
                            )
                            pos.tp_price = candidate.get("take_price")
                            pos.stop_price = candidate.get("stop_price")
                            pos.qty = qty
                            positions[symbol] = pos
                            print(f"[UserData recovery] {symbol}: recovered filled entry while local state was {state}.", flush=True)
                        else:
                            print(f"[UserData recovery] {symbol}: cannot recover filled entry (price={avg_price}, qty={qty}).", flush=True)
                            return
                    else:
                        print(f"[UserData] {symbol}: ignored {side} {order_type} update while local state is {state}.", flush=True)
                        return
                # получаем позицию
                if pos := positions.get(symbol):
                    # устанавливаем статус позиции
                    pos.status = True
                    if not getattr(pos, 'fill_time', 0.0):
                        pos.fill_time = time.time()
                    # определяем в чем начислена комиссия
                    comm_asset = o.get('N')
                    comm_amount = float(o.get('n', 0))
                    if comm_asset == 'BNB':
                        # если в BNB, то умножаем на цену
                        pos.commission += comm_amount * bnb_price
                    elif comm_asset == config['BOT']['ASSET']:
                        # если в asset, то просто прибавляем
                        pos.commission += comm_amount
                    # если ордер исполнен полностью
                    if status == 'FILLED':
                        # если исполнился ордер на вход в позицию
                        if side == 'BUY' and order_type == 'LIMIT':
                            # записываем сумму покупки (на фьючерсах z - накопленное количество)
                            pos.qty = float(o.get('z', 0))
                            if pos.qty > 0:
                                pos.quantity = pos.qty
                            # создаем задачу выставления тейка и стопа
                            asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity, getattr(pos, 'atr', None)))
                        # если исполнился ордер на выход из позиции
                        elif side == 'SELL':
                            # закрываем позицию
                            asyncio.create_task(trade_closed(o, pos))
        # изменение баланса на фьючерсах
        case 'ACCOUNT_UPDATE':
            update_data = msg.get('a', {})
            for coin in update_data.get('B', []):
                # если актив совпадает с нашим asset'ом
                if coin['a'] == config['BOT']['ASSET']:
                    # обновляем баланс (используем wb - Wallet Balance)
                    balance = float(coin['wb'])


# функция для выставления тейка и стопа
async def set_stop_take(symbol, entry_price, quantity, atr=None):
    global positions
    # получаем информацию о паре
    sym_info = all_symbols.get(symbol)
    if not sym_info:
        print(f"[ERROR] Symbol {symbol} not found in all_symbols during set_stop_take!")
        return
    
    pos = positions.get(symbol)
    if isinstance(pos, Position):
        take_price = pos.tp_price if pos.tp_price is not None else get_tp_sl_prices(entry_price, sym_info, atr)[0]
        stop_price = pos.stop_price if pos.stop_price is not None else get_tp_sl_prices(entry_price, sym_info, atr)[1]
    else:
        take_price, stop_price = get_tp_sl_prices(entry_price, sym_info, atr)

    # 1. ЗАПИСЫВАЕМ СДЕЛКУ В БД СРАЗУ, ЧТОБЫ ИЗБЕЖАТЬ ГОНКИ СОСТОЯНИЙ
    trade_id = None
    try:
        async with session() as s:
            # записываем сделку
            trade = db.Trades(symbol=symbol, order_size=entry_price * quantity, status='OPEN', open_time=utils.get_ts(),
                              entry_price=entry_price, quantity=quantity, take_price=take_price, stop_price=stop_price)
            # добавляем сделку
            s.add(trade)
            # сохраняем изменения в БД
            await s.commit()
            trade_id = trade.id
            clear_pending_entry(symbol)
    except Exception as e:
        print(f"Ошибка при записи сделки в БД по {symbol}: {e}")

    if conf.dry_run:
        # Отмечаем, что сделка сохранена в БД
        if pos := positions.get(symbol):
            if isinstance(pos, Position):
                pos.db_ready = True
                pos.tp_price = take_price
                pos.stop_price = stop_price
        print(f"[DRY RUN] Виртуальная сделка #{symbol} открыта в БД. TP: {take_price}, SL: {stop_price}")

    # 2. ОТПРАВЛЯЕМ СООБЩЕНИЕ В ТЕЛЕГРАМ АСИНХРОННО В ФОНЕ И ОБНОВЛЯЕМ TG_MESSAGE_ID В БД
    async def send_tg_and_update():
        prefix = "🤖 [DRY RUN] " if conf.dry_run else ""
        text = (f"{prefix}Открыл сделку на <b>{quantity}</b> #{symbol}\n"
                f"Вход: <b>{entry_price}</b>\n"
                f"Стоп/Тейк: <b>{stop_price} / {take_price}</b>")
        tg_message_id = None
        try:
            msg = await tg.bot.send_message(config.getint('TG', 'CHANNEL'), text)
            tg_message_id = msg.message_id
            if pos := positions.get(symbol):
                if isinstance(pos, Position):
                    pos.tg_message_id = tg_message_id
        except Exception as e:
            print(f"Не удалось отправить сообщение об открытии сделки в Telegram: {e}")
            
        if tg_message_id is not None and trade_id is not None:
            try:
                async with session() as s:
                    from sqlalchemy import update
                    await s.execute(update(db.Trades).where(db.Trades.id == trade_id).values(tg_message_id=tg_message_id))
                    await s.commit()
            except Exception as e:
                print(f"Не удалось обновить tg_message_id в БД для сделки {trade_id}: {e}")

    if pos := positions.get(symbol):
        if isinstance(pos, Position):
            pos.tg_task = asyncio.create_task(send_tg_and_update())
    else:
        asyncio.create_task(send_tg_and_update())

    if conf.dry_run:
        return


    try:
        # На фьючерсах выставляем Тейк Профит (LIMIT с reduceOnly=True)
        tp_order = await client.new_order(
            symbol=symbol,
            side='SELL',
            type='LIMIT',
            price=utils.round_price(take_price, sym_info.tick_size),
            quantity=quantity,
            reduceOnly='true',
            timeInForce='GTC'
        )
        # На фьючерсах выставляем Стоп Лосс (STOP_MARKET с closePosition=True)
        sl_order = await client.new_order(
            symbol=symbol,
            side='SELL',
            type='STOP_MARKET',
            stopPrice=utils.round_price(stop_price, sym_info.tick_size),
            closePosition='true'
        )
        print(f"Ордера для тейка и стопа установлены на фьючерсах: TP={tp_order.get('orderId')}, SL={sl_order.get('orderId')}")
        if pos := positions.get(symbol):
            if isinstance(pos, Position):
                pos.tp_order_id = tp_order.get('orderId')
                pos.sl_order_id = sl_order.get('orderId')
    except:
        print(f"Ошибка при размещение тейка и стопа по {symbol}, закрываю позицию\n{traceback.format_exc()}")
        # если ошибка, то закрываем позицию по рынку
        await client.new_order(symbol=symbol, side='SELL', quantity=quantity, type='MARKET', reduceOnly='true')
        return


# функция для обработки закрытия позиции
async def trade_closed(msg, pos):
    # Дожидаемся окончания отправки сообщения об открытии (если сделка закрылась очень быстро)
    if pos is not None and getattr(pos, 'tg_task', None) is not None:
        try:
            await pos.tg_task
        except Exception:
            pass

    # позиция закрыта
    trade = await db.get_trade(pos.symbol)
    # если нашли позицию
    if trade:
        # записываем время закрытия позиции
        trade.close_time = utils.get_ts()
        # считаем прибыль или убыток (используем rp - realized profit пришедший от биржи)
        trade.profit = float(msg.get('rp', 0)) - pos.commission
        # определяем тип закрытия и обновляем статус
        is_limit_exit = False
        msg_order_type = msg.get('o')
        msg_order_id = msg.get('i')
        
        if msg_order_type == 'LIMIT_EXIT':
            is_limit_exit = True
        elif msg_order_id is not None and pos is not None:
            if msg_order_id == getattr(pos, 'exit_order_id', None):
                is_limit_exit = True
            elif msg_order_type == 'LIMIT':
                tp_id = getattr(pos, 'tp_order_id', None)
                if tp_id is not None and msg_order_id != tp_id:
                    is_limit_exit = True
                elif tp_id is None:
                    # Если ID тейк-профита еще не сохранен в памяти, то исполненный лимитный ордер на продажу
                    # должен быть лимиткой защиты, так как тейк-профит не мог исполниться до его выставления
                    is_limit_exit = True

        if is_limit_exit:
            close_type = 'лимитке защиты'
            trade.status = 'CLOSED_MARKET'
        else:
            match msg_order_type:
                case 'LIMIT':
                    close_type = 'тейку'
                    trade.status = 'CLOSED_TAKE'
                case 'STOP_MARKET':
                    close_type = 'стопу'
                    trade.status = 'CLOSED_STOP'
                case _:
                    close_type = 'рынку'
                    trade.status = 'CLOSED_MARKET'
        # обновляем сделку
        await db.update_trade(trade)

        # В режиме реальной торговли отменяем ордера на бирже
        if not conf.dry_run:
            # отменяем оставшиеся ордера по этой паре (TP или SL)
            await cancel_orders(trade.symbol)
        else:
            # В режиме симуляции обновляем виртуальный баланс
            global balance
            balance += trade.profit
            print(f"[DRY RUN] Обновлен симуляционный баланс: {balance} USDT")

        # удаляем позицию из словаря
        if isinstance(pos, Position) and getattr(pos, 'pending_exit_task', None):
            try:
                pos.pending_exit_task.cancel()
            except:
                pass
        positions.pop(trade.symbol, None)
        clear_pending_entry(trade.symbol)
        
        # Обновляем кэш исходов сделок (v8.3+)
        close_time = trade.close_time or utils.get_ts()
        is_win = (trade.profit is not None and trade.profit > 0)
        if trade.symbol not in symbol_recent_trades:
            symbol_recent_trades[trade.symbol] = []
        symbol_recent_trades[trade.symbol].append((close_time, is_win))
        if len(symbol_recent_trades[trade.symbol]) > 10:
            symbol_recent_trades[trade.symbol].pop(0)
        # формируем сообщение
        prefix = "🤖 [DRY RUN] " if conf.dry_run else ""
        text = (f"{prefix}Закрыл сделку на <b>{trade.quantity}</b> #{trade.symbol} по {close_type}\n"
                f"{'Прибыль' if trade.profit > 0 else 'Убыток'}: <b>{utils.round_price(abs(trade.profit), 4)} {config['BOT']['ASSET']}</b>\n"
                f"Комиссия: <b>{utils.round_price(abs(pos.commission), 4)} {config['BOT']['ASSET']}</b>")
        # отправляем сообщение в телеграм канал как ответ на сообщение об открытии
        try:
            kwargs = {}
            tg_msg_id = getattr(pos, 'tg_message_id', None) or getattr(trade, 'tg_message_id', None)
            if tg_msg_id:
                kwargs['reply_parameters'] = {'message_id': tg_msg_id}
            await tg.bot.send_message(config.getint('TG', 'CHANNEL'), text, **kwargs)
        except Exception as e:
            # Резервный вариант, если оригинальное сообщение было удалено или возникла другая ошибка
            try:
                await tg.bot.send_message(config.getint('TG', 'CHANNEL'), text)
            except Exception as e2:
                print(f"Не удалось отправить сообщение о закрытии сделки: {e2}")


# функция для проверки позиции
async def check_positions():
    global positions
    global symbols_densities
    global balance
    global last_prices
    # вечный цикл
    while True:
        try:
            if conf.trade_mode:
                if positions:
                    print(f"[DEBUG] Активные позиции в памяти: { {sym: ('OPEN' if (isinstance(pos, Position) and pos.status) else 'LIMIT' if isinstance(pos, Position) else 'True' if pos is True else str(pos)) for sym, pos in positions.items() } }")
                    
                    # 1. Проверяем время удержания открытых позиций (hold time limit) (ОТКЛЮЧЕНО)
                    # for sym in list(positions.keys()):
                    #     pos = positions.get(sym)
                    #     if isinstance(pos, Position) and pos.status and pos.fill_time > 0:
                    #         elapsed = time.time() - pos.fill_time
                    #         if elapsed >= MAX_HOLD_TIME_SECONDS:
                    #             print(f"[Time limit] Позиция по {sym} удерживается {elapsed:.1f}с (лимит {MAX_HOLD_TIME_SECONDS}с). Закрываем по рынку.", flush=True)
                    #             if conf.dry_run:
                    #                 positions.pop(sym, None)
                    #                 current_price = last_prices.get(sym)
                    #                 if current_price is None:
                    #                     current_price = pos.entry_price
                    #                 fake_msg = {
                    #                     'o': 'MARKET_TIMEOUT',
                    #                     'rp': (current_price - pos.entry_price) * pos.quantity
                    #                 }
                    #                 pos.commission += current_price * pos.quantity * 0.0005
                    #                 asyncio.create_task(trade_closed(fake_msg, pos))
                    #             else:
                    #                 asyncio.create_task(close_position(sym))
                    #             continue

                    # В режиме симуляции (Dry Run) проверяем TP/SL по ценам из REST API, так как вебсокет тикеров может не работать
                    if conf.dry_run:
                        try:
                            tickers = await client.ticker_price()
                            current_prices = {item['symbol']: float(item['price']) for item in tickers}
                            
                            # перебираем копию ключей, так как positions может изменяться во время итерации
                            for sym in list(positions.keys()):
                                pos = positions.get(sym)
                                if isinstance(pos, Position):
                                    current_price = current_prices.get(sym)
                                    if current_price is None:
                                        continue
                                    
                                    # сохраняем цену в last_prices
                                    last_prices[sym] = current_price
                                    
                                    if not pos.status:
                                        # 1. Проверяем исполнение лимитки на покупку
                                        if current_price <= pos.entry_price:
                                            pos.status = True
                                            pos.fill_time = time.time()
                                            pos.commission = pos.entry_price * pos.quantity * 0.0002
                                            pos.qty = pos.quantity
                                            print(f"[DRY RUN] Лимитка по {sym} исполнена по цене {pos.entry_price} (в check_positions)")
                                            asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity, getattr(pos, 'atr', None)))
                                    else:
                                        # 2. Позиция открыта, проверяем достижение TP или SL
                                        if getattr(pos, 'db_ready', False):
                                            await check_and_apply_breakeven(sym, current_price)
                                            sym_info = all_symbols.get(sym)
                                            if not sym_info:
                                                print(f"[ERROR] Symbol {sym} not found in all_symbols during check_positions!")
                                                continue
                                            take_price = pos.tp_price if pos.tp_price is not None else get_tp_sl_prices(pos.entry_price, sym_info)[0]
                                            stop_price = pos.stop_price if pos.stop_price is not None else get_tp_sl_prices(pos.entry_price, sym_info)[1]
                                            
                                            if current_price >= take_price:
                                                print(f"[DRY RUN] Достигнут тейк-профит по {sym} на цене {current_price} (TP={take_price})")
                                                positions.pop(sym, None)
                                                fake_msg = {
                                                    'o': 'LIMIT',
                                                    'rp': (take_price - pos.entry_price) * pos.quantity
                                                }
                                                pos.commission += take_price * pos.quantity * 0.0002
                                                asyncio.create_task(trade_closed(fake_msg, pos))
                                                
                                            elif current_price <= stop_price:
                                                print(f"[DRY RUN] Достигнут стоп-лосс по {sym} на цене {current_price} (SL={stop_price})")
                                                positions.pop(sym, None)
                                                fake_msg = {
                                                    'o': 'STOP_MARKET',
                                                    'rp': (stop_price - pos.entry_price) * pos.quantity
                                                }
                                                pos.commission += stop_price * pos.quantity * 0.0005
                                                asyncio.create_task(trade_closed(fake_msg, pos))
                        except Exception as e:
                            print(f"[ERROR check_positions] Ошибка проверки цен виртуальных позиций: {e}")
                
                # перебираем позиции для отмены лимиток на вход при уходе плотности
                for sym, pos in list(positions.items()):
                    if isinstance(pos, Position):
                        if not pos.status:
                            density = symbols_densities.get(pos.symbol)
                            density_price, density_ts = get_density_price_ts(density)
                            if density_price is None or pos.density != density_price or utils.get_ts() - density_ts > conf.entry_timeout * 1000:
                                print(f"Cancel stale entry limit for {pos.symbol} at {pos.entry_price}")
                                clear_pending_entry(pos.symbol)
                                asyncio.create_task(cancel_limit(pos.symbol, pos.order_id))
                            continue
                        if not pos.status and (density := symbols_densities.get(pos.symbol)):
                            if pos.density != get_density_price_ts(density)[0] or utils.get_ts() - get_density_price_ts(density)[1] > conf.entry_timeout * 1000:
                                print(f"Отменяем лимитку на вход {pos.symbol} цена {pos.entry_price}")
                                asyncio.create_task(cancel_limit(pos.symbol, pos.order_id))
        except Exception as main_e:
            print(f"[ERROR check_positions] Критическая ошибка в check_positions: {main_e}")
            
        await asyncio.sleep(config.getint('BOT', 'CHECK_POSITION_TIMEOUT'))


# функция для отмены позиции
async def cancel_limit(symbol, order_id):
    global positions
    global last_prices
    if conf.dry_run:
        # Проверяем, не успела ли цена дойти до лимитки в симуляции
        current_price = last_prices.get(symbol)
        if pos := positions.get(symbol):
            if isinstance(pos, Position) and current_price and current_price <= pos.entry_price:
                # Исполняем лимитку вместо отмены!
                pos.status = True
                pos.fill_time = time.time()
                pos.commission = pos.entry_price * pos.quantity * 0.0002
                pos.qty = pos.quantity
                print(f"[DRY RUN] Лимитка по {symbol} исполнена по цене {pos.entry_price} при попытке отмены (цена {current_price})")
                asyncio.create_task(set_stop_take(pos.symbol, pos.entry_price, pos.quantity, getattr(pos, 'atr', None)))
                return
        print(f"[DRY RUN] Отменен лимит ордер по {symbol}")
        positions.pop(symbol, None)
        return
    try:
        # отменяем ордер
        await client.cancel_order(symbol=symbol, orderId=order_id)
        # Если отмена успешна, удаляем позицию
        positions.pop(symbol, None)
        print(f"Успешно отменили лимит ордер {order_id} по {symbol} на бирже")
    except binance.error.ClientException as e:
        if e.error_code == -2011:
            print(f"Не удалось отменить ордер {order_id} по {symbol}: ордер уже исполнен (Error -2011). Оставляем для отслеживания.")
            # НЕ удаляем из positions, чтобы дождаться подтверждения заполнения в msg_userdata
        else:
            print(f"Не удалось отменить ордер {order_id} по {symbol}: {e}")
            positions.pop(symbol, None)
    except Exception as e:
        print(f"Неизвестная ошибка при отмене ордера {order_id} по {symbol}: {e}")
        positions.pop(symbol, None)


# функция для сверки и восстановления состояния при запуске
async def reconcile_state():
    global positions
    print("[Бот] Запуск процесса сверки позиций...")
    try:
        open_trades = await db.get_all_trades()
        if conf.dry_run:
            for trade in open_trades:
                pos = Position(trade.symbol, trade.quantity, trade.entry_price, trade.entry_price)
                pos.status = True
                pos.fill_time = time.time() - 10.0
                pos.db_ready = True
                pos.tp_price = trade.take_price
                pos.stop_price = trade.stop_price
                positions[trade.symbol] = pos
                print(f"[Бот] [DRY RUN] Восстановлена виртуальная позиция в памяти: {trade.symbol} по цене {trade.entry_price}")
            return

        # В реальном режиме сверяем с биржей
        account_info = await client.account()
        exchange_positions = {}
        for p in account_info.get('positions', []):
            amt = float(p.get('positionAmt', 0))
            if amt != 0:
                exchange_positions[p['symbol']] = {
                    'amt': amt,
                    'entryPrice': float(p.get('entryPrice', 0))
                }

        for trade in open_trades:
            symbol = trade.symbol
            if symbol in exchange_positions:
                p_info = exchange_positions[symbol]
                pos = Position(symbol, trade.quantity, trade.entry_price, trade.entry_price)
                pos.status = True
                pos.fill_time = time.time() - 10.0
                pos.db_ready = True
                pos.tp_price = trade.take_price
                pos.stop_price = trade.stop_price
                positions[symbol] = pos
                print(f"[Бот] Восстановлена реальная позиция в памяти: {symbol} объем {p_info['amt']}")
            else:
                trade.close_time = utils.get_ts()
                trade.status = 'CLOSED_MARKET'
                try:
                    income_raw = await client.get_income_history(
                        symbol=symbol, incomeType="REALIZED_PNL",
                        startTime=trade.open_time, limit=10)
                    real_pnl = sum(float(r["income"]) for r in income_raw if int(r["time"]) >= trade.open_time)
                    trade.profit = real_pnl
                except:
                    trade.profit = 0.0
                await db.update_trade(trade)
                print(f"[Бот] Закрыта устаревшая сделка в БД: {symbol} (закрыта на бирже в офлайне)")

        for symbol, p_info in exchange_positions.items():
            if not any(t.symbol == symbol for t in open_trades):
                print(f"[Бот] Внимание! Обнаружена сиротская позиция на бирже без записи в БД: {symbol}")
                try:
                    await client.cancel_open_orders(symbol=symbol)
                    side = 'SELL' if p_info['amt'] > 0 else 'BUY'
                    await client.new_order(symbol=symbol, side=side, type='MARKET',
                                           quantity=abs(p_info['amt']), reduceOnly='true')
                    print(f"[Бот] Сиротская позиция {symbol} успешно закрыта на бирже для безопасности.")
                except Exception as e:
                    print(f"[Бот] Не удалось принудительно закрыть сиротскую позицию {symbol}: {e}")
    except Exception as e:
        print(f"[Бот] Ошибка сверки позиций: {e}\n{traceback.format_exc()}")


# функция для получения баланса
async def get_coins():
    coins = {}
    account_info = await client.account()
    # считываем активные позиции по полю positionAmt
    for pos in account_info.get('positions', []):
        amt = float(pos.get('positionAmt', 0))
        if amt != 0:
            # сохраняем размер позиции по символу
            coins[pos['symbol']] = abs(amt)
    # возвращаем результат
    return coins


# функция для закрытия позиции
# функция для закрытия позиции
async def close_position(symbol, limit_first=False):
    # Защитный таймер: минимальное время удержания позиции 3 секунды
    import time
    if symbol and symbol in positions:
        pos = positions[symbol]
        if isinstance(pos, Position) and pos.status:
            elapsed = time.time() - getattr(pos, 'fill_time', 0.0)
            if elapsed < MIN_POSITION_HOLD_SECONDS:
                print(f"[Защита комиссий] Отклонен запрос на закрытие позиции {symbol} по рынку: прошло всего {elapsed:.2f} сек с момента входа (требуется 3.0 сек)", flush=True)
                return

    # загружаем сделки
    trades = await db.get_all_trades()
    
    # Проверяем активные позиции в памяти на случай, если запись в БД еще не завершилась
    in_memory_symbols = []
    if symbol:
        if symbol in positions:
            pos = positions[symbol]
            if isinstance(pos, Position) and pos.status:
                in_memory_symbols.append((symbol, pos.quantity))
    else:
        for s, pos in positions.items():
            if isinstance(pos, Position) and pos.status:
                in_memory_symbols.append((s, pos.quantity))

    # создаем список задач
    tasks = []

    if conf.dry_run:
        # В режиме симуляции закрываем виртуально
        closed_symbols = set()
        for trade in trades:
            if (not symbol or trade.symbol == symbol):
                tasks.append(asyncio.create_task(close_dry_run_position(trade, limit_first=limit_first)))
                closed_symbols.add(trade.symbol)
        
        # Если в памяти есть то, чего еще нет в БД
        for s, qty in in_memory_symbols:
            if s not in closed_symbols:
                fake_trade = db.Trades(symbol=s, quantity=qty, entry_price=positions[s].entry_price, status='OPEN')
                tasks.append(asyncio.create_task(close_dry_run_position(fake_trade, limit_first=limit_first)))
    else:
        # загружаем монеты с биржи
        coins = await get_coins()
        
        # Сначала закрываем все сделки из БД
        closed_symbols = set()
        for trade in trades:
            # если пара совпадает или не указан
            if (not symbol or trade.symbol == symbol):
                # если позиция по символу открыта
                if trade.symbol in coins:
                    # создаем задачу
                    tasks.append(asyncio.create_task(close_pos_order(trade.symbol, coins[trade.symbol], limit_first=limit_first)))
                    closed_symbols.add(trade.symbol)
                    
        # Если на бирже есть позиция по symbol, но в БД записи еще нет, но в памяти она уже открыта
        for s, qty in in_memory_symbols:
            if s not in closed_symbols:
                if s in coins:
                    tasks.append(asyncio.create_task(close_pos_order(s, coins[s], limit_first=limit_first)))
                    
    # дожимаемся завершения задач
    if tasks:
        await asyncio.gather(*tasks)
    # если не указана пара и не симуляция
    if not symbol and not conf.dry_run:
        # отменяем все ордера
        await cancel_all_orders()


# функция для виртуального закрытия позиции в режиме Dry Run
async def close_dry_run_position(trade, limit_first=False):
    # Получаем позицию из словаря
    pos = positions.get(trade.symbol)
    if not pos or not isinstance(pos, Position):
        # Если почему-то нет в positions, но есть в БД
        # То создаем временный объект Position для корректного закрытия
        pos = Position(trade.symbol, trade.quantity, trade.entry_price, trade.entry_price)
        pos.status = True
        pos.commission = trade.entry_price * trade.quantity * 0.0002
        pos.tp_price = trade.take_price
        pos.stop_price = trade.stop_price
    
    # Нам нужно узнать текущую рыночную цену
    # Мы можем получить ее из тикеров
    current_price = pos.entry_price # fallback
    try:
        tickers = await client.ticker_price()
        for t in tickers:
            if t['symbol'] == trade.symbol:
                current_price = float(t['price'])
                break
    except:
        pass
        
    if limit_first:
        density_data = symbols_densities.get(trade.symbol, {})
        exit_price = density_data.get("best_ask")
        if not exit_price:
            exit_price = current_price
            
        print(f"[DRY RUN] Начинаем лимитный выход по {trade.symbol} цена {exit_price}")
        
        filled = False
        for _ in range(15):
            await asyncio.sleep(1.0)
            ticker_price = last_prices.get(trade.symbol)
            if ticker_price and ticker_price >= exit_price:
                filled = True
                pos.commission += exit_price * pos.quantity * 0.0002 # maker fee
                fake_msg = {
                    'o': 'LIMIT_EXIT',
                    'rp': (exit_price - pos.entry_price) * pos.quantity
                }
                positions.pop(trade.symbol, None)
                await trade_closed(fake_msg, pos)
                print(f"[DRY RUN] Лимитный выход по {trade.symbol} ИСПОЛНЕН по цене {exit_price}")
                break
                
        if not filled:
            # Закрываем по рынку по текущей цене по истечении 15 секунд
            fallback_price = last_prices.get(trade.symbol) or current_price
            pos.commission += fallback_price * pos.quantity * 0.0005 # taker fee
            fake_msg = {
                'o': 'MARKET',
                'rp': (fallback_price - pos.entry_price) * pos.quantity
            }
            positions.pop(trade.symbol, None)
            await trade_closed(fake_msg, pos)
            print(f"[DRY RUN] Лимитный выход по {trade.symbol} ИСТЕК. Закрываем по РЫНКУ по цене {fallback_price}")
    else:
        # Добавляем комиссию на выход по рынку (0.05% taker fee)
        pos.commission += current_price * pos.quantity * 0.0005
        
        fake_msg = {
            'o': 'MARKET',
            'rp': (current_price - pos.entry_price) * pos.quantity
        }
        # Сразу удаляем из positions
        positions.pop(trade.symbol, None)
        await trade_closed(fake_msg, pos)


# функция для закрытия позиции
async def close_pos_order(symbol, quantity, limit_first=False):
    try:
        sym_info = all_symbols.get(symbol)
        if not sym_info:
            print(f"[ERROR] Cannot close position for {symbol}: symbol info not found in all_symbols!")
            return
            
        if limit_first:
            print(f"Пытаемся закрыть позицию {symbol} на {quantity} лимитным ордером...")
            # Отменяем текущие TP/SL ордера по паре
            await cancel_orders(symbol)
            
            # Определяем цену лимитного выхода (best_ask для закрытия лонга)
            density_data = symbols_densities.get(symbol, {})
            exit_price = density_data.get("best_ask")
            if not exit_price:
                exit_price = last_prices.get(symbol)
            if not exit_price:
                try:
                    ticker = await client.ticker_price(symbol=symbol)
                    exit_price = float(ticker['price'])
                except:
                    print(f"[WARNING] Не удалось получить цену для лимитного выхода по {symbol}, закрываем по рынку.")
                    await client.new_order(symbol=symbol, side='SELL', type='MARKET',
                                           quantity=utils.round_down(quantity, sym_info.step_size),
                                           reduceOnly='true')
                    return
            
            # Выставляем лимитку на продажу
            try:
                exit_order = await client.new_order(
                    symbol=symbol,
                    side='SELL',
                    type='LIMIT',
                    price=utils.round_price(exit_price, sym_info.tick_size),
                    quantity=utils.round_down(quantity, sym_info.step_size),
                    reduceOnly='true',
                    timeInForce='GTC'
                )
                exit_order_id = exit_order['orderId']
                if pos := positions.get(symbol):
                    if isinstance(pos, Position):
                        pos.exit_order_id = exit_order_id
                print(f"Выставил лимитный ордер на закрытие {exit_order_id} по цене {exit_price}")
            except Exception as e:
                print(f"Не удалось выставить лимитный ордер на закрытие {symbol}: {e}. Закрываем по рынку.")
                await client.new_order(symbol=symbol, side='SELL', type='MARKET',
                                       quantity=utils.round_down(quantity, sym_info.step_size),
                                       reduceOnly='true')
                return
                
            # Ждем до 15 секунд, проверяя заполнение
            filled = False
            for _ in range(15):
                await asyncio.sleep(1.0)
                if symbol not in positions:
                    filled = True
                    break
                try:
                    status_order = await client.get_order(symbol=symbol, orderId=exit_order_id)
                    if status_order.get('status') == 'FILLED':
                        filled = True
                        break
                except Exception as e:
                    print(f"Ошибка проверки статуса ордера выхода {exit_order_id}: {e}")
                    
            if not filled:
                print(f"Лимитный ордер закрытия {exit_order_id} по {symbol} не исполнился за 15 секунд. Отменяем и выходим по рынку.")
                try:
                    await client.cancel_order(symbol=symbol, orderId=exit_order_id)
                except Exception as e:
                    print(f"Не удалось отменить лимитку закрытия {exit_order_id} (возможно, она только что исполнилась): {e}")
                    
                # Выходим по рынку (последний шанс)
                try:
                    coins = await get_coins()
                    if symbol in coins:
                        await client.new_order(symbol=symbol, side='SELL', type='MARKET',
                                               quantity=utils.round_down(coins[symbol], sym_info.step_size),
                                               reduceOnly='true')
                except Exception as e:
                    print(f"Ошибка рыночного закрытия {symbol} после таймаута лимитки: {e}")
        else:
            print(f"Закрываю позицию {symbol} на {quantity} по рынку")
            await cancel_orders(symbol)
            await client.new_order(symbol=symbol, side='SELL', type='MARKET',
                                   quantity=utils.round_down(quantity, sym_info.step_size),
                                   reduceOnly='true')
    except Exception as e:
        print(f"Failed to close {symbol} with reduce-only market order: {e}")



# функция для отмены ордеров
async def cancel_orders(symbol):
    try:
        # отменяем все открытые ордера по паре
        await client.cancel_open_orders(symbol=symbol)
    except:
        pass


# функция отмены всех ордеров
async def cancel_all_orders():
    symbols = []
    tasks = []
    # перебираем все ордера
    for order in (await client.get_orders()):
        # получаем пару
        symbol = order['symbol']
        # если еще не отменяли ордера для нее
        if symbol not in symbols:
            # запоминаем пару
            symbols.append(symbol)
            # отменяем ордера
            tasks.append(asyncio.create_task(cancel_orders(symbol)))
    # дожидаемся завершения задач
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
