import os
import csv
import io
import requests
import zipfile
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

# 📅 Генерация списка месяцев между start_date и end_date
def generate_months(start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m')
    end = datetime.strptime(end_date, '%Y-%m')
    months = []
    while start <= end:
        months.append(start.strftime('%Y-%m'))
        start += relativedelta(months=1)
    return months

# ⬇️ Загрузка свечей с Binance Vision
def download_klines(symbol, interval, start_date, end_date):
    months = generate_months(start_date, end_date)
    if not os.path.exists('klines'):
        os.makedirs('klines')

    klines = {
        'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': []
    }

    for month in months:
        filename = f"{symbol}-{interval}-{month}.zip"
        file_path = f"klines/{filename}"
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{interval}/{filename}"

        # Скачивание, если файла нет
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            try:
                r = requests.get(url)
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    f.write(r.content)
            except Exception as e:
                print(f"Ошибка при загрузке {filename}: {e}")
                continue

        # Распаковка CSV
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                csv_name = f"{symbol}-{interval}-{month}.csv"
                with zip_file.open(csv_name) as csv_file:
                    reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
                    for row in reader:
                        if row[0].isdigit():
                            klines['Date'].append(datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc))
                            klines['Open'].append(float(row[1]))
                            klines['High'].append(float(row[2]))
                            klines['Low'].append(float(row[3]))
                            klines['Close'].append(float(row[4]))
                            klines['Volume'].append(float(row[5]))
        except Exception as e:
            print(f"Ошибка чтения {file_path}: {e}")
            continue

    df = pd.DataFrame(klines)
    df.set_index('Date', inplace=True)
    return df




import os
import pandas as pd
import matplotlib.pyplot as plt
import talib

# Уровни считаются одинаковыми, если разница меньше этого
LEVEL_TOLERANCE = 0.002  # 0.2%
MIN_TOUCHES = 2
MAX_GAP_BETWEEN_TOUCHES = 20
MIN_REJECTION_PERCENT = 0.01  # 1%
REENTRY_TOLERANCE = 0.003  # 0.3% от уровня

def create_folder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

# def find_confirmed_levels(df, window=100):
#     levels = []
#     for i in range(window, len(df) - 1):
#         window_df = df.iloc[i - window:i]
#         current = df.iloc[i]
#         price = current['Close']
#
#         # === Поиск HIGH уровней ===
#         highs = window_df['High'].values
#         highs_touched = []
#         for j in range(len(highs)):
#             for h in highs_touched:
#                 if abs(highs[j] - h) / h < LEVEL_TOLERANCE:
#                     break
#             else:
#                 # ищем касания в пределах 0.2%
#                 level = highs[j]
#                 count = sum(abs(highs - level) / level < LEVEL_TOLERANCE)
#                 if count >= MIN_TOUCHES:
#                     # ищем откат от уровня
#                     indices = [k for k, v in enumerate(highs) if abs(v - level)/level < LEVEL_TOLERANCE]
#                     valid = True
#                     for idx in indices:
#                         sub = window_df.iloc[idx:idx+MAX_GAP_BETWEEN_TOUCHES]
#                         if not any((level - sub['Low']) / level > MIN_REJECTION_PERCENT):
#                             valid = False
#                             break
#                     if valid:
#                         highs_touched.append(level)
#                         if abs(price - level)/level < REENTRY_TOLERANCE and price < level:
#                             levels.append({'type': 'buy', 'level': level, 'timestamp': df.index[i]})
#
#         # === Поиск LOW уровней ===
#         lows = window_df['Low'].values
#         lows_touched = []
#         for j in range(len(lows)):
#             for l in lows_touched:
#                 if abs(lows[j] - l) / l < LEVEL_TOLERANCE:
#                     break
#             else:
#                 level = lows[j]
#                 count = sum(abs(lows - level) / level < LEVEL_TOLERANCE)
#                 if count >= MIN_TOUCHES:
#                     indices = [k for k, v in enumerate(lows) if abs(v - level)/level < LEVEL_TOLERANCE]
#                     valid = True
#                     for idx in indices:
#                         sub = window_df.iloc[idx:idx+MAX_GAP_BETWEEN_TOUCHES]
#                         if not any((sub['High'] - level) / level > MIN_REJECTION_PERCENT):
#                             valid = False
#                             break
#                     if valid:
#                         lows_touched.append(level)
#                         if abs(price - level)/level < REENTRY_TOLERANCE and price > level:
#                             levels.append({'type': 'sell', 'level': level, 'timestamp': df.index[i]})
#
#     return levels
#
# def generate_trigger_orders(levels, buffer=0.001, rr1=1, rr2=2):
#     signals = []
#     for lvl in levels:
#         price = lvl['level']
#         ts = lvl['timestamp']
#
#         if lvl['type'] == 'buy':
#             entry = price * (1 + buffer)
#             sl = price * (1 - buffer)
#             tp1 = entry + (entry - sl) * rr1
#             tp2 = entry + (entry - sl) * rr2
#         else:
#             entry = price * (1 - buffer)
#             sl = price * (1 + buffer)
#             tp1 = entry - (sl - entry) * rr1
#             tp2 = entry - (sl - entry) * rr2
#
#         signals.append({
#             'timestamp': ts,
#             'type': lvl['type'],
#             'entry': entry,
#             'stop': sl,
#             'tp1': tp1,
#             'tp2': tp2,
#             'level': price
#         })
#     return pd.DataFrame(signals)
#



# def plot_signal_chart(df, signal, symbol='ASSET', folder='charts'):
#     create_folder(folder)
#     ts = signal['timestamp']
#     entry = signal['entry']
#     stop = signal['stop']
#     tp1 = signal['tp1']
#     tp2 = signal['tp2']
#     level = signal['level']
#     sig_type = signal['type']
#
#     start = ts - pd.Timedelta(hours=4)
#     end = ts + pd.Timedelta(hours=4)
#     day_df = df[(df.index >= start) & (df.index <= end)]
#
#     if day_df.empty:
#         return
#
#     fig, ax = plt.subplots(figsize=(14, 6))
#     ax.plot(day_df.index, day_df['Close'], label='Close', color='black', linewidth=1)
#
#     ax.axhline(entry, color='green', linestyle='--', label='Entry')
#     ax.axhline(stop, color='red', linestyle='--', label='Stop Loss')
#     ax.axhline(tp1, color='blue', linestyle='--', label='Take Profit 1')
#     ax.axhline(tp2, color='purple', linestyle='--', label='Take Profit 2')
#     ax.axhline(level, color='orange', linestyle=':', label='Trigger Level')
#
#     direction = 'BUY STOP' if sig_type == 'buy' else 'SELL STOP'
#     ax.set_title(f"{symbol} | {ts.strftime('%Y-%m-%d %H:%M')} | {direction}")
#     ax.legend()
#     ax.grid(True)
#     fig.tight_layout()
#
#     fname = f"{folder}/{symbol}_{ts.strftime('%Y-%m-%d_%H-%M')}.png"
#     fig.savefig(fname)
#     plt.close()



# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import os
# import pandas as pd
#
# def create_folder(folder):
#     if not os.path.exists(folder):
#         os.makedirs(folder)
#
# def plot_signal_chart(df, signal, symbol='ASSET', folder='charts'):
#     create_folder(folder)
#
#     ts = signal['timestamp']
#     entry = signal['entry']
#     stop = signal['stop']
#     tp1 = signal['tp1']
#     tp2 = signal.get('tp2', None)  # optional
#     level = signal.get('level', None)  # optional
#     sig_type = signal['type']
#
#     # Отображаем 1 неделю до сигнала и 2 дня после
#     start = ts - pd.Timedelta(days=7)
#     end = ts + pd.Timedelta(days=2)
#     week_df = df[(df.index >= start) & (df.index <= end)]
#
#     if week_df.empty:
#         print(f"No data in the range for {symbol} at {ts}")
#         return
#
#     fig, ax = plt.subplots(figsize=(16, 6))
#     ax.plot(week_df.index, week_df['Close'], label='Close', color='black', linewidth=1)
#
#     # Горизонтальные уровни
#     ax.axhline(entry, color='green', linestyle='--', label='Entry')
#     ax.axhline(stop, color='red', linestyle='--', label='Stop Loss')
#     ax.axhline(tp1, color='blue', linestyle='--', label='Take Profit 1')
#
#     if tp2:
#         ax.axhline(tp2, color='purple', linestyle='--', label='Take Profit 2')
#     if level:
#         ax.axhline(level, color='orange', linestyle=':', label='Trigger Level')
#
#     # Вертикальная линия сигнала
#     ax.axvline(ts, color='gray', linestyle=':', linewidth=1)
#
#     # Формат оси X
#     ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d\n%H:%M'))
#     ax.xaxis.set_major_locator(mdates.AutoDateLocator())
#
#     direction = 'BUY STOP' if sig_type == 'buy' else 'SELL STOP'
#     ax.set_title(f"{symbol} | {ts.strftime('%Y-%m-%d %H:%M')} | {direction}")
#     ax.legend(loc='upper left')
#     ax.grid(True)
#
#     fig.tight_layout()
#
#     # Сохранение
#     fname = f"{folder}/{symbol}_{ts.strftime('%Y-%m-%d_%H-%M')}.png"
#     fig.savefig(fname)
#     plt.close()










import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import pandas as pd

def create_folder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

def plot_signal_chart(df, signal, symbol='ASSET', folder='charts'):
    create_folder(folder)

    ts = signal['timestamp']
    entry = signal['entry']
    stop = signal['stop']
    tp1 = signal['tp1']
    tp2 = signal.get('tp2', None)
    level = signal.get('level', None)
    sig_type = signal['type']

    # Отображаем 1 неделю до и 2 дня после сигнала
    start = ts - pd.Timedelta(days=7)
    end = ts + pd.Timedelta(days=2)
    week_df = df[(df.index >= start) & (df.index <= end)]

    if week_df.empty:
        print(f"No data in the range for {symbol} at {ts}")
        return

    fig, ax = plt.subplots(figsize=(16, 6))
    ax.plot(week_df.index, week_df['Close'], label='Close', color='black', linewidth=1)

    # Горизонтальные уровни
    ax.axhline(entry, color='green', linestyle='--', label='Entry')
    ax.axhline(stop, color='red', linestyle='--', label='Stop Loss')
    ax.axhline(tp1, color='blue', linestyle='--', label='Take Profit 1')

    if tp2:
        ax.axhline(tp2, color='purple', linestyle='--', label='Take Profit 2')
    if level:
        ax.axhline(level, color='orange', linestyle=':', label='Trigger Level')

    # Маркеры: точки на графике
    ax.plot(ts, entry, marker='o', color='green', label='Entry Point', markersize=8)
    ax.plot(ts, stop, marker='x', color='red', label='Stop Point', markersize=8)
    ax.plot(ts, tp1, marker='^', color='blue', label='TP1 Point', markersize=8)

    if tp2:
        ax.plot(ts, tp2, marker='v', color='purple', label='TP2 Point', markersize=8)

    # Вертикальная линия сигнала
    ax.axvline(ts, color='gray', linestyle=':', linewidth=1)

    # Оформление осей и подписи
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d\n%H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    direction = 'BUY STOP' if sig_type == 'buy' else 'SELL STOP'
    ax.set_title(f"{symbol} | {ts.strftime('%Y-%m-%d %H:%M')} | {direction}")
    ax.legend(loc='upper left')
    ax.grid(True)

    fig.tight_layout()

    # Сохранение
    fname = f"{folder}/{symbol}_{ts.strftime('%Y-%m-%d_%H-%M')}.png"
    fig.savefig(fname)
    plt.close()







ATR_PERIOD = 14


def find_confirmed_levels(df, window=100):
    levels = []
    for i in range(window, len(df) - 1):
        window_df = df.iloc[i - window:i]
        current = df.iloc[i]
        price = current['Close']

        # === Поиск HIGH уровней ===
        highs = window_df['High'].values
        highs_touched = []
        for j in range(len(highs)):
            for h in highs_touched:
                if abs(highs[j] - h) / h < LEVEL_TOLERANCE:
                    break
            else:
                level = highs[j]
                count = sum(abs(highs - level) / level < LEVEL_TOLERANCE)
                if count >= MIN_TOUCHES:
                    indices = [k for k, v in enumerate(highs) if abs(v - level)/level < LEVEL_TOLERANCE]
                    valid = True
                    for idx in indices:
                        sub = window_df.iloc[idx:idx+MAX_GAP_BETWEEN_TOUCHES]
                        if not any((level - sub['Low']) / level > MIN_REJECTION_PERCENT):
                            valid = False
                            break
                    if valid:
                        highs_touched.append(level)
                        if abs(price - level)/level < REENTRY_TOLERANCE and price < level:
                            levels.append({'type': 'buy', 'level': level, 'timestamp': df.index[i]})

        # === Поиск LOW уровней ===
        lows = window_df['Low'].values
        lows_touched = []
        for j in range(len(lows)):
            for l in lows_touched:
                if abs(lows[j] - l) / l < LEVEL_TOLERANCE:
                    break
            else:
                level = lows[j]
                count = sum(abs(lows - level) / level < LEVEL_TOLERANCE)
                if count >= MIN_TOUCHES:
                    indices = [k for k, v in enumerate(lows) if abs(v - level)/level < LEVEL_TOLERANCE]
                    valid = True
                    for idx in indices:
                        sub = window_df.iloc[idx:idx+MAX_GAP_BETWEEN_TOUCHES]
                        if not any((sub['High'] - level) / level > MIN_REJECTION_PERCENT):
                            valid = False
                            break
                    if valid:
                        lows_touched.append(level)
                        if abs(price - level)/level < REENTRY_TOLERANCE and price > level:
                            levels.append({'type': 'sell', 'level': level, 'timestamp': df.index[i]})

    return levels


def generate_trigger_orders(levels, df, atr_period=ATR_PERIOD, slrr=0.5, rr1=1, rr2=2):
    df['ATR'] = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=atr_period)

    signals = []
    for lvl in levels:
        ts = lvl['timestamp']
        if ts not in df.index:
            continue

        atr = df.loc[ts, 'ATR']
        if pd.isna(atr) or atr == 0:
            continue

        price = lvl['level']
        if lvl['type'] == 'buy':
            entry = price * (1 + 0.001)
            sl = entry - atr * slrr
            tp1 = entry + atr * rr1
            tp2 = entry + atr * rr2
        else:
            entry = price * (1 - 0.001)
            sl = entry + atr * slrr
            tp1 = entry - atr * rr1
            tp2 = entry - atr * rr2

        signals.append({
            'timestamp': ts,
            'type': lvl['type'],
            'entry': entry,
            'stop': sl,
            'tp1': tp1,
            'tp2': tp2,
            'level': price,
            'atr': atr
        })

    return pd.DataFrame(signals)












df = download_klines('ETHUSDT', '4h', '2025-05', '2025-06')  # твоя функция

levels = find_confirmed_levels(df)
signals = generate_trigger_orders(levels, df)

print(f"Найдено сигналов: {len(signals)}")
for _, sig in signals.iterrows():
    plot_signal_chart(df, sig, symbol='ETHUSDT')


