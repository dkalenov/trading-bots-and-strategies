from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import os
import requests
import zipfile
import csv
import io
import pandas as pd
import numpy as np
import backtesting
from backtesting.lib import crossover

# ===== CONFIG =====
SYMBOL = 'BTCUSDT'
INTERVAL = '1h'
START_DATE = '2020-01'
END_DATE = '2024-9'
FAST_EMA = 12
SLOW_EMA = 26
CASH = 10000
COMMISSION = 0.002


def ema(data, period):
    """Compute EMA compatible with backtesting.py indicator wrapper."""
    result = np.full_like(data, np.nan, dtype=float)
    if len(data) < period:
        return result
    # SMA seed
    result[period - 1] = np.mean(data[:period])
    alpha = 2.0 / (period + 1)
    for i in range(period, len(data)):
        result[i] = (data[i] - result[i - 1]) * alpha + result[i - 1]
    return result


def generate_months(start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m')
    end = datetime.strptime(end_date, '%Y-%m')
    months = []
    while start <= end:
        months.append(start.strftime('%Y-%m'))
        start += relativedelta(months=1)
    return months


def download_klines(symbol, interval, months):
    os.makedirs('klines', exist_ok=True)
    klines = {'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': []}

    for month in months:
        filename = f"{symbol}-{interval}-{month}.zip"
        filepath = f"klines/{filename}"

        if not os.path.exists(filepath):
            url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{interval}/{filename}"
            print(f"Downloading {filename}...")
            r = requests.get(url, allow_redirects=True)
            with open(filepath, 'wb') as f:
                f.write(r.content)

        with zipfile.ZipFile(filepath, 'r') as zip_file:
            with zip_file.open(f"{symbol}-{interval}-{month}.csv", 'r') as csv_file:
                csv_reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
                for row in csv_reader:
                    if row[0].isdigit():
                        klines['Date'].append(datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc))
                        klines['Open'].append(float(row[1]))
                        klines['High'].append(float(row[2]))
                        klines['Low'].append(float(row[3]))
                        klines['Close'].append(float(row[4]))
                        klines['Volume'].append(float(row[5]))

    return pd.DataFrame(klines)


class EMA_Cross(backtesting.Strategy):
    fast_ema_period = FAST_EMA
    slow_ema_period = SLOW_EMA

    def init(self):
        self.fast_ema = self.I(ema, self.data.Close, self.fast_ema_period)
        self.slow_ema = self.I(ema, self.data.Close, self.slow_ema_period)

    def next(self):
        if crossover(self.fast_ema, self.slow_ema):
            self.position.close()
            self.buy()
        elif crossover(self.slow_ema, self.fast_ema):
            self.position.close()
            self.sell()


if __name__ == '__main__':
    months = generate_months(START_DATE, END_DATE)
    df = download_klines(SYMBOL, INTERVAL, months)

    # Set datetime index (required by backtesting.py)
    df.set_index('Date', inplace=True)

    print(f'Data: {len(df)} candles from {df.index[0]} to {df.index[-1]}')
    print(df.tail())

    bt = backtesting.Backtest(df, EMA_Cross, cash=CASH, commission=COMMISSION)
    stats = bt.run()
    print(stats)
    bt.plot()
