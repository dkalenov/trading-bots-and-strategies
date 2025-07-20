from datetime import datetime, timezone, timedelta
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL
from db import save_signal
from binance_data import *
import json
import os
import requests
from datetime import datetime, timezone
import time
import logging
import pandas as pd
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# TIMEFRAMES = ['4h', '30m', '1h']

TIMEFRAMES = ['4h']

INTERVAL_MAPPING = {
    "1m": Interval.INTERVAL_1_MINUTE,
    "5m": Interval.INTERVAL_5_MINUTES,
    "15m": Interval.INTERVAL_15_MINUTES,
    "30m": Interval.INTERVAL_30_MINUTES,
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
}

VIETNAM_TZ = timezone(timedelta(hours=7))
IMPORTANT_SYMBOLS = {"BTCUSDT", "ETHUSDT"}

signals = {tf: {"longs": {}, "shorts": {}} for tf in TIMEFRAMES}
signals["important"] = {}


CACHE_FILE = "tradingview_symbols.json"
# SUCCESS_FILE = "successful_signals_4h_BTC.csv"


success_symbols_df = pd.read_csv("successful_signals_4h_BTC.csv")
success_symbols = success_symbols_df['symbol'].unique().tolist()


def get_available_symbols():
    """ Получает список доступных символов на TradingView и Binance Futures и сохраняет его в JSON """
    binance_symbols = get_binance_symbols()
    available_symbols = []

    for symbol in binance_symbols:
        try:
            # Проверяем, доступен ли символ на TradingView (Binance Spot)
            handler = TA_Handler(symbol=symbol, exchange="Binance", screener="crypto", interval=Interval.INTERVAL_1_HOUR)
            handler.get_analysis()

            # Проверяем, доступен ли символ на Binance Futures
            futures_info = client.mark_price(symbol)
            if futures_info:  # Если Binance вернул данные, значит символ есть в фьючерсах
                available_symbols.append(symbol)

        except Exception as e:
            logging.warning(f"Символ {symbol} пропущен: {e}")
            continue  # Если ошибка, значит символа нет

    # Сохраняем в JSON
    with open(CACHE_FILE, "w") as f:
        json.dump(available_symbols, f)

    logging.info(f"Сохранено {len(available_symbols)} символов в {CACHE_FILE}")
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


def get_data(symbol, timeframe, retries=1):
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

            logging.warning(f"TV ошибка {symbol} ({timeframe}), попытка {attempt + 1}: {e}\n{traceback.format_exc()}")
            time.sleep(1)
    return None


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
        while True:
            try:
                response = requests.get(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    params={
                        "chat_id": TELEGRAM_CHANNEL,
                        "text": msg,
                        "parse_mode": "HTML"
                    }
                )
                if response.status_code == 429:  # Слишком много запросов
                    retry_after = response.json().get("parameters", {}).get("retry_after", 5)
                    logging.error(f"Flood control, ждём {retry_after} секунд...")
                    time.sleep(retry_after + 1)
                else:
                    break
            except Exception as e:
                logging.error(f"Ошибка отправки сообщения в Telegram: {e}")
                time.sleep(5)



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



# Функция загрузки свечей (1h, 4h, 30m и т.д.)
def fetch_klines(symbol, interval="4h", limit=150):
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time", "Open", "High", "Low", "Close", "Volume", "_", "_", "_", "_", "_", "_"
        ])
        df = df[["time", "Open", "High", "Low", "Close", "Volume"]].astype(float)
        df["Date"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.set_index("Date", inplace=True)
        return df
    except Exception as e:
        logging.error(f"Ошибка загрузки свечей для {symbol}: {e}")
        return pd.DataFrame()

# Функция расчёта ATR вручную (как в TA-Lib)
def calculate_atr(df, period=14):
    if df.empty:
        return df  # Возвращаем пустой DataFrame, если данных нет

    df["H-L"] = df["High"] - df["Low"]
    df["H-C"] = abs(df["High"] - df["Close"].shift(1))
    df["L-C"] = abs(df["Low"] - df["Close"].shift(1))
    df["TR"] = df[["H-L", "H-C", "L-C"]].max(axis=1)

    df["ATR"] = df["TR"].rolling(window=period, min_periods=1).mean()  # min_periods=1 позволяет избежать NaN
    return df.drop(columns=["H-L", "H-C", "L-C", "TR"])



def calculate_stop(signal, close_price, atr):
    # Лонг: SL ниже входа
    if signal == "STRONG_BUY":
        stop = round(close_price - (atr * 0.45), 8)
    # Шорт: SL выше входа
    else:
        stop = round(close_price + (atr * 0.45), 8)

    return stop


def calculate_take(signal, close_price, atr):
    # Лонг: SL ниже входа, TP выше входа
    if signal == "STRONG_BUY":
        take = round(close_price + (atr * 1.25), 8)
    # Шорт: SL выше входа
    else:
        take = round(close_price - (atr * 1.25), 8)

    return take


import html

def format_atr_signals_message(atr_signals):
    """Форматирует сигналы ATR, SL, TP в список сообщений Telegram (< 4000 символов)"""
    max_length = 4000
    messages = []
    current_msg = ""

    for symbol, data in atr_signals.items():
        direction = "LONG" if data.get("signal") == "STRONG_BUY" else "SHORT"

        try:
            entry = data.get("entry_price", "N/A")
            atr = f"{data.get('ATR', 0):.4f}"
            sl = f"{data.get('SL', 0):.4f}" if data.get("SL") is not None else "N/A"
            tp = f"{data.get('TP', 0):.4f}" if data.get("TP") is not None else "N/A"
            tf = data.get("timeframe", "N/A")

            msg = (
                f"<b>{html.escape(symbol)} {direction} {tf}</b>\n"
                f"📍 Вход: {entry}\n"
                f"📉 ATR: {atr}\n"
                f"🛑 SL: {sl}\n"
                f"🎯 TP: {tp}\n\n"
            )

            # Разбиение на части по размеру
            if len(current_msg) + len(msg) > max_length:
                messages.append(current_msg)
                current_msg = msg
            else:
                current_msg += msg

        except Exception as e:
            logging.error(f"Ошибка форматирования сигнала для {symbol}: {e}")

    if current_msg:
        messages.append(current_msg)

    return messages


def process_symbols(symbols, timeframe):
    start_time = time.time()
    global signals

    logging.info(f"Запуск process_symbols для {len(symbols)} символов на {timeframe}")

    prices = get_prices_binance(symbols)
    # success_symbols = load_successful_symbols()
    btc_signal = get_data("BTCUSDT", timeframe).get("RECOMMENDATION", "NEUTRAL")

    btc_long_signals = {"NEUTRAL", "BUY", "STRONG_BUY"}
    btc_short_signals = {"NEUTRAL", "SELL", "STRONG_SELL"}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_data, symbol, timeframe): symbol for symbol in symbols}

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                data = future.result()
                if not data:
                    logging.warning(f"{symbol}: Данных нет, пропускаем.")
                    continue

                signal = data.get("RECOMMENDATION", "NEUTRAL")
                # print('SIGNAL', signal)
                entry_price = prices.get(symbol)

                if entry_price is None:
                    logging.warning(f"Цена для {symbol} не найдена, пропускаем")
                    continue

                if signal in {"STRONG_BUY"}:
                    signals[timeframe]["longs"][symbol] = (signal, entry_price)
                elif signal in {"STRONG_SELL"}:
                    signals[timeframe]["shorts"][symbol] = (signal, entry_price)

                # Важные символы
                if symbol in IMPORTANT_SYMBOLS:
                    signals["important"][symbol] = (signal, entry_price)


                df = fetch_klines(symbol, interval=timeframe, limit=150)

                if df.empty:
                    logging.warning(f"{symbol}: DataFrame пуст, пропускаем")
                    continue

                required_cols = ["High", "Low", "Close"]
                if not all(col in df.columns for col in required_cols):
                    logging.warning(f"{symbol}: Отсутствуют нужные колонки {required_cols}, пропускаем")
                    continue

                df = calculate_atr(df)

                if df is None or "ATR" not in df.columns:
                    logging.warning(f"{symbol}: Функция calculate_atr не вернула ATR, пропускаем.")
                    continue

                if df.shape[0] < 2:
                    logging.error(f"{symbol}: Недостаточно данных после calculate_atr, пропускаем.")
                    continue

                last_row = df.iloc[-1]

                if any(pd.isna(last_row[col]) for col in ["High", "Low", "Close", "ATR"]):
                    logging.warning(f"{symbol}: В последней строке есть NaN, пропускаем. Данные:\n{last_row}")
                    continue

                atr = atr = round(float(last_row["ATR"]), 8)
                if pd.isna(atr):
                    logging.warning(f"{symbol}: ATR оказался NaN или None, пропускаем сохранение.")
                    continue

                # logging.info(f"Сохраняем сигнал: {symbol}, {timeframe}, {signal}, {entry_price}, {atr}")


                if symbol in IMPORTANT_SYMBOLS or signal in {"STRONG_BUY", "STRONG_SELL", 'BUY', 'SELL', "NEUTRAL"}:
                    save_signal(symbol, timeframe, signal, entry_price, atr)

                if (symbol in signals[timeframe]["longs"] or symbol in signals[timeframe]["shorts"]) and symbol in success_symbols:
                # if signal in {"STRONG_BUY", "STRONG_SELL"} and symbol in success_symbols:

                # Сигналы по BTC
                    is_btc_long = btc_signal in btc_long_signals
                    is_btc_short = btc_signal in btc_short_signals

                    # Если BTC даёт STRONG_BUY, то разрешаем только лонги по альтам
                    if signal == "STRONG_BUY" and is_btc_long or signal == "STRONG_SELL" and is_btc_short:
                    # Вычисляем TP и SL
                        take_profit = calculate_take(signal=signal, close_price=entry_price, atr=atr)
                        stop_loss = calculate_stop(signal=signal, close_price=entry_price, atr=atr)

                        # Сохраняем сигнал
                        atr_signal_data = {
                            "signal": signal,
                            "timeframe": timeframe,
                            "entry_price": entry_price,
                            "ATR": atr,
                            "SL": stop_loss,
                            "TP": take_profit
                        }

                        signals[timeframe].setdefault("atr_signals", {})[symbol] = atr_signal_data


            except Exception as e:
                logging.error(f"Ошибка обработки {symbol}: {e}\n{traceback.format_exc()}")
                logging.error(f"{symbol}: Ошибка при обработке, последние данные:\n{df.tail() if 'df' in locals() else 'DataFrame не найден'}")

    formatted_messages = format_signals(signals)
    for msg in formatted_messages:
        send_message(msg)
        time.sleep(3.5)

    if "atr_signals" in signals[timeframe]:
        message = format_atr_signals_message(signals[timeframe]["atr_signals"])
        if message:
            send_message(message)
            time.sleep(3.5)

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