import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL
from db import save_signal

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TIMEFRAMES = ['30m', '1h', '4h']
INTERVAL_MAPPING = {
    "1m": Interval.INTERVAL_1_MINUTE,
    "5m": Interval.INTERVAL_5_MINUTES,
    "15m": Interval.INTERVAL_15_MINUTES,
    "30m": Interval.INTERVAL_30_MINUTES,
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
}

client = UMFutures()
VIETNAM_TZ = timezone(timedelta(hours=7))
IMPORTANT_SYMBOLS = {"BTCUSDT", "ETHUSDT"}

signals = {tf: {"longs": {}, "shorts": {}} for tf in TIMEFRAMES}
signals["important"] = {}


import json
import os

CACHE_FILE = "tradingview_symbols.json"

def get_available_symbols():
    """ Получает список доступных символов на TradingView и сохраняет его в JSON """
    binance_symbols = get_symbols()  # Загружаем список всех символов с Binance
    available_symbols = []

    for symbol in binance_symbols:
        try:
            # Проверяем, доступен ли символ на TradingView
            handler = TA_Handler(symbol=symbol, exchange="Binance", screener="crypto", interval=Interval.INTERVAL_1_HOUR)
            handler.get_analysis()
            available_symbols.append(symbol)
        except:
            continue  # Если ошибка, значит символа нет на TV

    # Сохраняем в JSON
    with open(CACHE_FILE, "w") as f:
        json.dump(available_symbols, f)

    return available_symbols


def load_cached_symbols():
    """ Загружает символы из кэша, если файл существует """
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            symbols = json.load(f)
            logging.info(f"Загружено {len(symbols)} символов из {CACHE_FILE}")
            return symbols
    logging.warning("Файл кэша символов не найден, получаем заново.")
    return None




def get_data(symbol, timeframe, retries=3):
    """ Получает данные с TradingView с повторными попытками """
    interval = INTERVAL_MAPPING[timeframe]
    for attempt in range(retries):
        try:
            output = TA_Handler(
                symbol=symbol, exchange='Binance', screener="crypto", interval=interval
            )
            activity = output.get_analysis().summary
            activity['SYMBOL'] = symbol
            return activity
        except Exception as e:
            logging.warning(f"Ошибка получения данных {symbol} ({timeframe}), попытка {attempt+1}: {e}")
            time.sleep(1)
    return None



def get_symbols():
    """ Получает список доступных символов с Binance """
    try:
        tickers = client.mark_price()
        return [ticker['symbol'] for ticker in tickers if 'USDC' not in ticker['symbol'] and 'USDT' in ticker['symbol']]
    except Exception as e:
        logging.error(f"Ошибка получения списка символов: {e}")
        return []


def send_message(message):
    """ Отправляет сообщение в Telegram с учетом лимита 4096 символов """
    max_length = 4000
    messages = []

    if len(message) > max_length:
        parts = message.split("\n")
        chunk = ""

        for part in parts:
            if len(chunk) + len(part) + 1 > max_length:
                messages.append(chunk)
                chunk = part
            else:
                chunk += "\n" + part

        if chunk:
            messages.append(chunk)
    else:
        messages.append(message)

    for msg in messages:
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                params={
                    "chat_id": TELEGRAM_CHANNEL,
                    "text": msg,
                    "parse_mode": "HTML"  # Используем HTML для форматирования
                }
            )
            if response.status_code != 200:
                logging.error(f"Ошибка отправки в Telegram: {response.text}")
        except Exception as e:
            logging.error(f"Ошибка отправки сообщения в Telegram: {e}")


def format_signals(signals):
    """ Форматирует сигналы для Telegram в виде HTML """
    messages = []

    for tf in TIMEFRAMES:
        if not signals.get(tf, {}).get("longs") and not signals.get(tf, {}).get("shorts"):
            continue

        msg = f"📊 Signals for {tf} timeframe:\n"

        # Важные символы (BTC, ETH)
        for symbol, (signal, price) in signals.get("important", {}).items():
            msg += f"{symbol}: {signal} at {price}\n"

        # Longs
        if signals[tf].get("longs"):
            msg += "\n🚀 **Longs:**\n"
            for symbol, (signal, price) in signals[tf]["longs"].items():
                msg += f"{symbol}: price {price}\n"

        # Shorts
        if signals[tf].get("shorts"):
            msg += "\n📉 **Shorts:**\n"
            for symbol, (signal, price) in signals[tf]["shorts"].items():
                msg += f"{symbol}: price {price}\n"

        messages.append(msg)

    return messages


def process_symbols(symbols, timeframe):
    """ Обрабатывает символы и сохраняет сигналы """
    start_time = time.time()
    global signals

    # Загружаем кэшированный список символов
    available_symbols = load_cached_symbols()
    if not available_symbols:
        available_symbols = get_available_symbols()  # Если нет кэша, загружаем заново

    # Оставляем только символы, которые есть в TradingView
    symbols = [s for s in symbols if s in available_symbols]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_data, symbol, timeframe): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                data = future.result()
                if not data:
                    continue
                signal = data.get("RECOMMENDATION", "NEUTRAL")
                entry_price = float(client.mark_price(symbol)["markPrice"])

                # Сохраняем сигналы
                if signal in {"STRONG_BUY"}:
                    signals[timeframe]["longs"][symbol] = (signal, entry_price)
                elif signal in {"STRONG_SELL"}:
                    signals[timeframe]["shorts"][symbol] = (signal, entry_price)

                # Важные символы
                if symbol in IMPORTANT_SYMBOLS:
                    signals["important"][symbol] = (signal, entry_price)

                if symbol in IMPORTANT_SYMBOLS or signal in {"STRONG_BUY", "STRONG_SELL"}:
                    save_signal(symbol, timeframe, signal, entry_price)

            except Exception as e:
                logging.error(f"Ошибка обработки {symbol}: {e}")

    formatted_messages = format_signals(signals)
    for msg in formatted_messages:
        send_message(msg)

    logging.info(f"Обработка {len(symbols)} символов заняла {time.time() - start_time:.2f} секунд")



def wait_for_next_candle(timeframe):
    """ Ожидает открытия следующей свечи """
    now_utc = datetime.now(timezone.utc)
    if "h" in timeframe:
        tf_hours = int(timeframe[:-1])
        next_candle_hour = (now_utc.hour // tf_hours + 1) * tf_hours
        next_candle_time_utc = now_utc.replace(hour=next_candle_hour % 24, minute=0, second=0, microsecond=0)
        if next_candle_hour >= 24:
            next_candle_time_utc += timedelta(days=1)
    elif "m" in timeframe:
        tf_minutes = int(timeframe[:-1])
        next_candle_minute = (now_utc.minute // tf_minutes + 1) * tf_minutes
        next_candle_time_utc = now_utc.replace(second=0, microsecond=0) + timedelta(minutes=(next_candle_minute - now_utc.minute))
    wait_time = max((next_candle_time_utc - now_utc).total_seconds(), 0)
    logging.info(f"Ждем {int(wait_time)} сек до следующей {timeframe} свечи")
    time.sleep(wait_time)


def monitor_timeframe(timeframe):
    global signals
    while True:
        # Загружаем кэш символов перед началом мониторинга
        available_symbols = load_cached_symbols()
        if not available_symbols:
            available_symbols = get_available_symbols()

        wait_for_next_candle(timeframe)
        process_symbols(available_symbols, timeframe)  # Запускаем обработку только в нужное время



        # Очищаем сигналы для всех таймфреймов
        for tf in TIMEFRAMES:
            signals[tf] = {"longs": {}, "shorts": {}}


if __name__ == "__main__":
    logging.info('START')
    send_message('START')

    with ThreadPoolExecutor(max_workers=len(TIMEFRAMES)) as executor:
        futures = {executor.submit(monitor_timeframe, tf): tf for tf in TIMEFRAMES}

        try:
            for future in as_completed(futures):
                tf = futures[future]
                try:
                    future.result()  # If a thread fails, this will raise an exception
                except Exception as e:
                    error_msg = f"❌ Error in thread {tf}: {e}"
                    logging.error(error_msg)
                    send_message(error_msg)  # Send the error message to Telegram
        except KeyboardInterrupt:
            logging.info("Program stopped (Ctrl+C)")
            send_message("⛔ Program stopped (Ctrl+C)")
