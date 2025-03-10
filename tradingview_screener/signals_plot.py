import os
import pandas as pd
import requests
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from sqlalchemy.util import symbol

# csv_file = "tradingview_signals_server.csv"
# start_date = "2025-03-01"
# end_date = "2025-03-09"
# interval = "1h"  # Binance использует строковые интервалы
#
# def download_klines_daily(symbol, interval, start_date, end_date):
#     start_date = datetime.strptime(start_date, '%Y-%m-%d')
#     end_date = datetime.strptime(end_date, '%Y-%m-%d')
#
#     klines = {
#         'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': [], 'Symbol': []
#     }
#
#     limit = 1500  # Binance API лимит на 1 запрос
#     current_date = start_date
#
#     while current_date <= end_date:
#         start_time = int(current_date.timestamp() * 1000)
#         end_time = int(end_date.timestamp() * 1000)  # Окончание загрузки
#
#         url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit={limit}"
#
#         try:
#             r = requests.get(url)
#             r.raise_for_status()
#             data = r.json()
#             if not data:
#                 print(f"⚠️ Нет данных для {symbol} в {current_date.strftime('%Y-%m-%d')}")
#                 break
#
#             for row in data:
#                 klines['Date'].append(datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc))
#                 klines['Open'].append(float(row[1]))
#                 klines['High'].append(float(row[2]))
#                 klines['Low'].append(float(row[3]))
#                 klines['Close'].append(float(row[4]))
#                 klines['Volume'].append(float(row[5]))
#                 klines['Symbol'].append(symbol)
#
#             # Обновляем `current_date`, двигаясь вперед на `limit` интервалов
#             current_date += timedelta(hours=limit)
#             time.sleep(0.01)
#
#         except Exception as e:
#             print(f"❌ Ошибка при загрузке {symbol}: {e}")
#             break
#
#     if not klines['Date']:
#         print(f"⚠️ Нет данных для {symbol} в диапазоне {start_date} - {end_date}")
#         return pd.DataFrame()
#
#     df = pd.DataFrame(klines)
#     df['Date'] = pd.to_datetime(df['Date'])
#     df.set_index('Date', inplace=True)
#
#     return df
#
# # === Читаем CSV и получаем список уникальных символов ===
# signals_df = pd.read_csv(csv_file)
# symbols = signals_df['symbol'].unique().tolist()  # Теперь список символов
#
# # === Загружаем данные для всех символов ===
# dfs = []
# for symbol in symbols:
#     print(f"📥 Загружаем данные для {symbol}...")
#     df = download_klines_daily(symbol, interval, start_date, end_date)
#     if not df.empty:
#         dfs.append(df)
#
# # === Объединяем данные и сохраняем ===
# if dfs:
#     df_all = pd.concat(dfs)
#     df_all.to_csv("klines_data.csv")
#     print(f"✅ Данные сохранены в klines_data.csv")
# else:
#     print("⚠️ Данные не были загружены.")




df = pd.read_csv("klines_data.csv", parse_dates=['Date'], index_col='Date')

# === Функция для отрисовки свечного графика ===
def plot_candlestick(df, symbol):
    df_symbol = df[df['Symbol'] == symbol].copy()

    # Убираем колонку 'Symbol', чтобы передать корректный DataFrame в mplfinance
    df_symbol = df_symbol.drop(columns=['Symbol'])

    # Настройки графика
    mpf.plot(
        df_symbol,
        type='candle',  # Японские свечи
        style='charles',  # Стиль графика
        title=f"Свечной график {symbol}",
        ylabel="Цена (USDT)",
        volume=True,  # Отображение объема
        figsize=(12, 6),
    )

# === Визуализация для каждого символа ===
symbols = df['Symbol'].unique()

# for symbol in symbols:
#     print(f"📊 Отрисовка графика для {symbol}...")
#     plot_candlestick(df, symbol)
#     plt.show()

# def save_candlestick_plot(df, symbol, interval):
#     df_symbol = df[df['Symbol'] == symbol].copy()
#     df_symbol = df_symbol.drop(columns=['Symbol'])  # Удаляем колонку 'Symbol'
#
#     # Создаём папки, если их нет
#     base_dir = "plots"
#     symbol_dir = os.path.join(base_dir, symbol)
#     timeframe_dir = os.path.join(symbol_dir, f"{symbol}_{interval}")
#
#     os.makedirs(timeframe_dir, exist_ok=True)
#
#     # Путь для сохранения изображения
#     plot_path = os.path.join(timeframe_dir, f"{symbol}_{interval}.png")
#
#     # === Построение графика ===
#     fig, axlist = mpf.plot(
#         df_symbol,
#         type='candle',  # Японские свечи
#         style='charles',  # Стиль графика
#         title=f"Свечной график {symbol}",
#         ylabel="Цена (USDT)",
#         volume=True,  # Добавляем объём
#         figsize=(12, 6),
#         returnfig=True  # Возвращаем фигуру и оси
#     )
#
#     # Сохранение графика
#     fig.savefig(plot_path)
#     plt.close(fig)
#     print(f"✅ График сохранён: {plot_path}")
#
#
# # === Обрабатываем каждый символ ===
# symbols = df['Symbol'].unique()
# interval = "1h"  # Укажите нужный таймфрейм
#
# for symbol in symbols:
#     save_candlestick_plot(df, symbol, interval)


import os
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt

import pandas as pd
import os
import matplotlib.pyplot as plt
import mplfinance as mpf

# === Чтение данных свечей ===
# df = pd.read_csv("klines_data.csv", parse_dates=['Date'], index_col='Date')
#
# # Проверяем, есть ли у индекса таймзона
# if df.index.tz is None:
#     df.index = df.index.tz_localize('UTC')
# else:
#     df.index = df.index.tz_convert('UTC')
#
# # Проверяем, есть ли нужные столбцы для mplfinance
# ohlc_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
# if not all(col in df.columns for col in ohlc_columns):
#     raise ValueError("❌ В DataFrame отсутствуют необходимые столбцы: 'Open', 'High', 'Low', 'Close', 'Volume'")
#
# # === Чтение данных сигналов ===
# signals_df = pd.read_csv("tradingview_signals_server.csv", parse_dates=['utc_time'])
# signals_df.rename(columns={'utc_time': 'Date'}, inplace=True)
#
# # Обрабатываем таймзону
# if signals_df['Date'].dt.tz is None:
#     signals_df['Date'] = signals_df['Date'].dt.tz_localize('UTC')
# else:
#     signals_df['Date'] = signals_df['Date'].dt.tz_convert('UTC')
#
#
# # === Функция для сохранения графиков с сигналами ===
# def save_candlestick_plot(df, signals_df, symbol, interval):
#     df_symbol = df[df['Symbol'] == symbol].copy()
#     df_symbol = df_symbol.drop(columns=['Symbol'])
#
#     # Отбираем сигналы по символу и таймфрейму
#     signals_filtered = signals_df[
#         (signals_df['symbol'] == symbol) &
#         (signals_df['timeframe'] == interval)
#     ].copy()
#
#     # Фильтруем сигналы, оставляя только те, что попадают в диапазон дат свечек
#     signals_filtered = signals_filtered[signals_filtered['Date'].between(df_symbol.index.min(), df_symbol.index.max())]
#
#     if signals_filtered.empty:
#         print(f"⚠️ Нет сигналов для {symbol} ({interval}) в доступных данных.")
#         return
#     else:
#         print(f"✅ Найдено {len(signals_filtered)} сигналов для {symbol} ({interval}).")
#
#     # Создаём папки для хранения графиков
#     base_dir = "plots"
#     symbol_dir = os.path.join(base_dir, symbol)
#     timeframe_dir = os.path.join(symbol_dir, f"{symbol}_{interval}")
#     os.makedirs(timeframe_dir, exist_ok=True)
#
#     # Путь для сохранения изображения
#     plot_path = os.path.join(timeframe_dir, f"{symbol}_{interval}.png")
#
#     # Создание графика с двумя подграфиками: свечи + объем
#     fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
#
#     # Рисуем свечной график + объем
#     mpf.plot(df_symbol, type='candle', style='charles', ax=ax1, volume=ax2)
#
#     # Добавление сигналов на график
#     for _, row in signals_filtered.iterrows():
#         color = 'green' if row['signal'] == 'STRONG_BUY' else 'red'
#         marker = '^' if row['signal'] == 'STRONG_BUY' else 'v'
#         ax1.scatter(row['Date'], row['entry_price'], color=color, marker=marker, s=100)
#
#         # Добавляем подпись рядом с сигналом
#         ax1.text(row['Date'], row['entry_price'], f"{row['entry_price']:.2f}",
#                  fontsize=10, verticalalignment='bottom' if row['signal'] == 'STRONG_BUY' else 'top',
#                  color=color, bbox=dict(facecolor='white', edgecolor=color, boxstyle='round,pad=0.3'))
#
#     # Сохранение графика
#     plt.savefig(plot_path)
#     plt.close()
#     print(f"✅ График сохранён: {plot_path}")
#
#
# # === Генерация графиков для всех символов ===
# symbols = df['Symbol'].unique()
# interval = "1h"  # Укажите нужный таймфрейм
#
# for symbol in symbols:
#     save_candlestick_plot(df, signals_df, symbol, interval)


import requests
import pandas as pd
import os
import matplotlib.pyplot as plt
import mplfinance as mpf
from matplotlib.dates import AutoDateLocator, DateFormatter

# === Чтение данных свечей ===
df = pd.read_csv("klines_data.csv", parse_dates=['Date'], index_col='Date')
df.index = pd.to_datetime(df.index, utc=True)

# Проверка необходимых столбцов
ohlc_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Symbol']
if not all(col in df.columns for col in ohlc_columns):
    raise ValueError(
        "❌ В DataFrame отсутствуют необходимые столбцы: 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'")

# === Чтение данных сигналов ===
signals_df = pd.read_csv("tradingview_signals_server.csv")
signals_df['Date'] = pd.to_datetime(signals_df['utc_time'], errors='coerce', utc=True)

if signals_df['Date'].isna().any():
    raise ValueError("❌ Ошибка: В столбце 'utc_time' есть некорректные значения.")


# === Функция для создания графика ===
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# def save_candlestick_plot(df, signals_df, symbol, interval):
#     df_symbol = df[df['Symbol'] == symbol].copy()
#     df_symbol = df_symbol.drop(columns=['Symbol'])
#
#     # Проверка корректности данных
#     print(f"Доступные даты в данных для {symbol}: {df_symbol.index.min()} — {df_symbol.index.max()}")
#
#     # Фильтруем сигналы
#     signals_filtered = signals_df[(signals_df['symbol'] == symbol) & (signals_df['timeframe'] == interval)]
#     signals_filtered = signals_filtered[signals_filtered['Date'].between(df_symbol.index.min(), df_symbol.index.max())]
#
#     if signals_filtered.empty:
#         print(f"⚠️ Нет сигналов для {symbol} ({interval})")
#     else:
#         print(f"✅ Найдено {len(signals_filtered)} сигналов для {symbol} ({interval})")
#
#     # Создаём график
#     fig, ax = plt.subplots(figsize=(18, 8))
#
#     # Рисуем свечной график вручную
#     for i in range(len(df_symbol)):
#         open_, high, low, close = df_symbol.iloc[i][['Open', 'High', 'Low', 'Close']]
#         color = 'green' if close >= open_ else 'red'
#
#         # Тень свечи
#         ax.plot([df_symbol.index[i], df_symbol.index[i]], [low, high], color='black', linewidth=1)
#
#         # Тело свечи
#         ax.plot([df_symbol.index[i], df_symbol.index[i]], [open_, close], color=color, linewidth=5)
#
#     # Добавляем сигналы
#     for _, row in signals_filtered.iterrows():
#         color = 'green' if row['signal'] == 'STRONG_BUY' else 'red'
#         marker = '^' if row['signal'] == 'STRONG_BUY' else 'v'
#         ax.scatter(row['Date'], row['entry_price'], color=color, marker=marker, s=120, edgecolors='black', zorder=3)
#         ax.text(row['Date'], row['entry_price'], row['signal'].replace("STRONG_", ""),
#                 fontsize=10, verticalalignment='bottom' if row['signal'] == 'STRONG_BUY' else 'top',
#                 color=color, bbox=dict(facecolor='white', edgecolor=color, boxstyle='round,pad=0.3'))
#
#     # Настраиваем оси
#     ax.xaxis.set_major_locator(mdates.AutoDateLocator())
#     ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d, %H:%M'))
#     plt.xticks(rotation=30)
#     plt.grid(True, linestyle='--', alpha=0.5)
#
#     # Сохраняем график
#     base_dir = "plots"
#     os.makedirs(base_dir, exist_ok=True)
#     plot_path = os.path.join(base_dir, f"{symbol}_{interval}.png")
#     plt.savefig(plot_path, dpi=300)
#     plt.close()
#
#     print(f"✅ График сохранён: {plot_path}")


import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def save_candlestick_plot(df, signals_df, symbol, interval):
    df_symbol = df[df['Symbol'] == symbol].copy()
    df_symbol = df_symbol.drop(columns=['Symbol'])

    # Группируем данные по дням
    df_symbol['date_only'] = df_symbol.index.date
    unique_dates = df_symbol['date_only'].unique()

    # Проверка корректности данных
    print(f"Доступные даты в данных для {symbol}: {df_symbol.index.min()} — {df_symbol.index.max()}")

    # Фильтруем сигналы
    signals_filtered = signals_df[(signals_df['symbol'] == symbol) & (signals_df['timeframe'] == interval)]
    signals_filtered = signals_filtered[signals_filtered['Date'].between(df_symbol.index.min(), df_symbol.index.max())]

    if signals_filtered.empty:
        print(f"⚠️ Нет сигналов для {symbol} ({interval})")
    else:
        print(f"✅ Найдено {len(signals_filtered)} сигналов для {symbol} ({interval})")

    # Создаём папки для хранения графиков
    base_dir = os.path.join("plots", symbol, f"{symbol}_{interval}")
    os.makedirs(base_dir, exist_ok=True)

    for date in unique_dates:
        df_day = df_symbol[df_symbol['date_only'] == date]

        if df_day.empty:
            continue

        signals_day = signals_filtered[signals_filtered['Date'].dt.date == date]

        # Создаём график
        fig, ax = plt.subplots(figsize=(18, 8))

        # Рисуем свечной график вручную
        for i in range(len(df_day)):
            open_, high, low, close = df_day.iloc[i][['Open', 'High', 'Low', 'Close']]
            color = 'green' if close >= open_ else 'red'

            # Тень свечи
            ax.plot([df_day.index[i], df_day.index[i]], [low, high], color='black', linewidth=1)

            # Тело свечи
            ax.plot([df_day.index[i], df_day.index[i]], [open_, close], color=color, linewidth=5)

        # Добавляем сигналы
        for _, row in signals_day.iterrows():
            color = 'green' if row['signal'] == 'STRONG_BUY' else 'red'
            marker = '^' if row['signal'] == 'STRONG_BUY' else 'v'

            ax.scatter(row['Date'], row['entry_price'], color=color, marker=marker, s=120, edgecolors='black', zorder=3)

            # Текст с сигналом и ценой входа
            ax.text(row['Date'], row['entry_price'],
                    f"{row['signal'].replace('STRONG_', '')}\n{row['entry_price']:.2f}",
                    fontsize=10, verticalalignment='bottom' if row['signal'] == 'STRONG_BUY' else 'top',
                    color=color, bbox=dict(facecolor='white', edgecolor=color, boxstyle='round,pad=0.3'))

        # Настраиваем оси
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xticks(rotation=30)
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.title(f"{symbol} {interval} — {date}")

        # Сохраняем график
        plot_path = os.path.join(base_dir, f"{symbol}_{interval}_{date}.png")
        plt.savefig(plot_path, dpi=300)
        plt.close()

        print(f"✅ График сохранён: {plot_path}")


# === Генерация графиков ===
symbols = df['Symbol'].unique()
interval = "1h"

for symbol in symbols:
    save_candlestick_plot(df, signals_df, symbol, interval)





