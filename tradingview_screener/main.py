import time
import requests
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from binance.um_futures import UMFutures
from tradingview_ta import TA_Handler, Interval, Exchange
from config import TELEGRAM_TOKEN, TELEGRAM_CHANNEL
from db import save_signal

TIMEFRAMES = ["5m", "15m"]
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


def get_data(symbol, timeframe):
    interval = INTERVAL_MAPPING[timeframe]
    output = TA_Handler(
        symbol=symbol,
        exchange='Binance',
        screener="crypto",
        interval=interval
    )
    activity = output.get_analysis().summary
    activity['SYMBOL'] = symbol
    return activity


def get_symbols():
    tickers = client.mark_price()
    return [ticker['symbol'] for ticker in tickers if 'USDC' not in ticker['symbol'] and 'USDT' in ticker['symbol']]


def send_message(message):
    requests.get(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        params={"chat_id": TELEGRAM_CHANNEL, "text": message}
    )


def wait_for_next_candle(timeframe):
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
        next_candle_time_utc = now_utc.replace(second=0, microsecond=0) + timedelta(
            minutes=(next_candle_minute - now_utc.minute))

    wait_time = (next_candle_time_utc - now_utc).total_seconds()
    print(f"Waiting for next {timeframe} candle: {int(wait_time)} seconds")

    if wait_time > 0:
        time.sleep(wait_time)


def process_symbols(symbols, timeframe):
    global signals
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {executor.submit(get_data, symbol, timeframe): symbol for symbol in symbols}

        for future in future_to_symbol:
            try:
                data = future.result()
                symbol = future_to_symbol[future]
                signal = data.get("RECOMMENDATION", "NEUTRAL")
                entry_price = float(client.mark_price(symbol)["markPrice"])

                # BTC и ETH записываем всегда (даже если сигнал НЕ "STRONG_BUY"/"STRONG_SELL")
                if symbol in IMPORTANT_SYMBOLS:
                    save_signal(symbol, timeframe, signal, entry_price)
                    signals[timeframe][
                        "longs" if signal == "STRONG_BUY" else "shorts"
                    ][symbol] = (signal, entry_price)

                # Остальные монеты записываем ТОЛЬКО если сигнал "STRONG_BUY" / "STRONG_SELL"
                elif signal in {"STRONG_BUY", "STRONG_SELL"}:
                    signals[timeframe]["longs" if signal == "STRONG_BUY" else "shorts"][symbol] = (signal, entry_price)

            except Exception as e:
                print(f"Error processing {symbol}: {e}")
            time.sleep(0.01)



def format_signals():
    messages = []

    for i, tf in enumerate(TIMEFRAMES):
        if not signals[tf]["longs"] and not signals[tf]["shorts"]:
            continue

        sub_tf = TIMEFRAMES[i - 1] if i > 0 else None
        msg = f"📊 Signals for {tf} timeframe:\n"

        # BTC и ETH
        for symbol in IMPORTANT_SYMBOLS:
            if symbol in signals[tf]["longs"] or symbol in signals[tf]["shorts"]:
                signal, price = signals[tf]["longs"].get(symbol, signals[tf]["shorts"].get(symbol))
                msg += f"{symbol}: {signal} at {price}\n"

        # Longs
        if signals[tf]["longs"]:
            msg += "\n🚀 **Longs:**\n"
            for symbol, (signal, price) in signals[tf]["longs"].items():
                if sub_tf and symbol in signals.get(sub_tf, {}).get("longs", {}):
                    sub_signal = signals[sub_tf]["longs"][symbol][0]
                    msg += f"{symbol}: {signal} at {price} ({sub_signal} on {sub_tf})\n"
                else:
                    msg += f"{symbol}: {signal} at {price}\n"

        # Shorts
        if signals[tf]["shorts"]:
            msg += "\n📉 **Shorts:**\n"
            for symbol, (signal, price) in signals[tf]["shorts"].items():
                if sub_tf and symbol in signals.get(sub_tf, {}).get("shorts", {}):
                    sub_signal = signals[sub_tf]["shorts"][symbol][0]
                    msg += f"{symbol}: {signal} at {price} ({sub_signal} on {sub_tf})\n"
                else:
                    msg += f"{symbol}: {signal} at {price}\n"

        messages.append(msg)

    return messages


def monitor_timeframe(timeframe):
    while True:
        wait_for_next_candle(timeframe)
        symbols = get_symbols()
        process_symbols(symbols, timeframe)

        if timeframe == TIMEFRAMES[-1]:  # Отправка сообщений после завершения последнего таймфрейма
            messages = format_signals()
            for msg in messages:
                send_message(msg)

            # Очищаем сигналы для всех таймфреймов
            for tf in TIMEFRAMES:
                signals[tf] = {"longs": {}, "shorts": {}}


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=len(TIMEFRAMES)) as executor:
        executor.map(monitor_timeframe, TIMEFRAMES)

