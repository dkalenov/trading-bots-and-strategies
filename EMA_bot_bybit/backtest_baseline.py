"""EMA Crossover Baseline — with SL/TP and visual charts.

Baseline version: EMA(12/26) on BTCUSDT 1h.
Adds: stop-loss, take-profit, entry/exit markers on charts.
"""
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import os
import requests
import zipfile
import csv
import io
import numpy as np
import pandas as pd
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

# SL/TP as ATR multiplier
ATR_PERIOD = 14
SL_ATR = 1.5   # stop-loss = 1.5 * ATR
TP_ATR = 3.0   # take-profit = 3.0 * ATR


def ema(data, period):
    """Compute EMA compatible with backtesting.py."""
    result = np.full_like(data, np.nan, dtype=float)
    if len(data) < period:
        return result
    result[period - 1] = np.mean(data[:period])
    alpha = 2.0 / (period + 1)
    for i in range(period, len(data)):
        result[i] = (data[i] - result[i - 1]) * alpha + result[i - 1]
    return result


def atr(highs, lows, closes, period=14):
    """Compute ATR compatible with backtesting.py."""
    result = np.full_like(closes, np.nan, dtype=float)
    if len(closes) < 2:
        return result
    trs = np.zeros(len(closes))
    for i in range(1, len(closes)):
        trs[i] = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
    # Simple moving average of TR
    for i in range(period, len(closes)):
        result[i] = np.mean(trs[i - period + 1:i + 1])
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


class EMA_Baseline(backtesting.Strategy):
    """EMA Crossover with SL/TP based on ATR."""
    fast_ema_period = FAST_EMA
    slow_ema_period = SLOW_EMA
    atr_period = ATR_PERIOD
    sl_atr = SL_ATR
    tp_atr = TP_ATR

    def init(self):
        self.fast_ema = self.I(ema, self.data.Close, self.fast_ema_period)
        self.slow_ema = self.I(ema, self.data.Close, self.slow_ema_period)
        self.atr = self.I(atr, self.data.High, self.data.Low, self.data.Close, self.atr_period)
        self._entry_price = None

    def next(self):
        current_atr = self.atr[-1]
        if np.isnan(current_atr) or current_atr <= 0:
            return

        if self.position:
            entry = self._entry_price
            if entry is None:
                return
            if self.position.is_long:
                sl = entry - self.sl_atr * current_atr
                tp = entry + self.tp_atr * current_atr
                if self.data.Low[-1] <= sl:
                    self.position.close()
                    self._entry_price = None
                elif self.data.High[-1] >= tp:
                    self.position.close()
                    self._entry_price = None
            else:
                sl = entry + self.sl_atr * current_atr
                tp = entry - self.tp_atr * current_atr
                if self.data.High[-1] >= sl:
                    self.position.close()
                    self._entry_price = None
                elif self.data.Low[-1] <= tp:
                    self.position.close()
                    self._entry_price = None
        else:
            if crossover(self.fast_ema, self.slow_ema):
                self.buy()
                self._entry_price = self.data.Close[-1]
            elif crossover(self.slow_ema, self.fast_ema):
                self.sell()
                self._entry_price = self.data.Close[-1]


if __name__ == '__main__':
    months = generate_months(START_DATE, END_DATE)
    df = download_klines(SYMBOL, INTERVAL, months)
    df.set_index('Date', inplace=True)

    print(f'Data: {len(df)} candles from {df.index[0]} to {df.index[-1]}')

    bt = backtesting.Backtest(df, EMA_Baseline, cash=CASH, commission=COMMISSION)
    stats = bt.run()
    print("\n=== BASELINE RESULTS (EMA 12/26 + SL/TP) ===")
    print(stats)

    # Save stats to file
    with open('baseline_results.txt', 'w') as f:
        f.write(str(stats))
    print("\nStats saved to baseline_results.txt")

    # Plot and save chart
    try:
        bt.plot(filename='baseline_chart.html', open_browser=False)
        print("Chart saved to baseline_chart.html")
    except Exception as e:
        print(f"Chart plot error (non-critical): {e}")
