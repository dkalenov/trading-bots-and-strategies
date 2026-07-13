"""Generate 3 example charts with candlesticks and EMA crossover entries."""
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import os
import requests
import zipfile
import csv
import io
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf


def ema(data, period):
    result = np.full_like(data, np.nan, dtype=float)
    if len(data) < period:
        return result
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


def find_crossovers(df, fast_period=12, slow_period=26):
    """Find all crossover points."""
    fast = ema(df['Close'].values, fast_period)
    slow = ema(df['Close'].values, slow_period)
    
    crossovers = []
    for i in range(1, len(df)):
        if np.isnan(fast[i]) or np.isnan(slow[i]) or np.isnan(fast[i-1]) or np.isnan(slow[i-1]):
            continue
        if fast[i] > slow[i] and fast[i-1] <= slow[i-1]:
            crossovers.append((df.index[i], 'LONG', df['Close'].iloc[i]))
        elif fast[i] < slow[i] and fast[i-1] >= slow[i-1]:
            crossovers.append((df.index[i], 'SHORT', df['Close'].iloc[i]))
    
    return crossovers, fast, slow


def plot_candlestick(df, fast, slow, crossover_point, title, filename):
    """Plot candlestick chart with EMA and entry marker."""
    # Compute EMAs for the window
    fast_series = pd.Series(fast, index=df.index)
    slow_series = pd.Series(slow, index=df.index)
    
    # EMA plots
    ema_fast = mpf.make_addplot(fast_series, color='dodgerblue', width=1.5, label='EMA(12)')
    ema_slow = mpf.make_addplot(slow_series, color='orange', width=1.5, label='EMA(26)')
    
    # Entry marker
    date, direction, price = crossover_point
    marker_data = pd.Series(np.nan, index=df.index)
    marker_data[date] = price
    
    if direction == 'LONG':
        marker_color = 'lime'
        marker_marker = '^'
    else:
        marker_color = 'red'
        marker_marker = 'v'
    
    entry_plot = mpf.make_addplot(
        marker_data, type='scatter', markersize=150,
        marker=marker_marker, color=marker_color
    )
    
    # Style
    mc = mpf.make_marketcolors(
        up='green', down='red',
        edge='inherit',
        wick='inherit',
        volume='in'
    )
    style = mpf.make_mpf_style(
        marketcolors=mc,
        gridstyle='--',
        gridcolor='gray',
        y_on_right=False
    )
    
    # Plot
    fig, axes = mpf.plot(
        df, type='candle', style=style,
        volume=True,
        addplot=[ema_fast, ema_slow, entry_plot],
        title=title,
        ylabel='Price (USDT)',
        ylabel_lower='Volume',
        figsize=(14, 8),
        returnfig=True
    )
    
    # Add legend
    axes[0].legend(['EMA(12)', 'EMA(26)', f'{direction} Entry'], loc='upper left')
    
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {filename}")


if __name__ == '__main__':
    months = generate_months('2023-01', '2024-09')
    df = download_klines('BTCUSDT', '1h', months)
    df.set_index('Date', inplace=True)
    
    print(f"Data: {len(df)} candles")
    
    crossovers, fast, slow = find_crossovers(df)
    print(f"Found {len(crossovers)} crossovers")
    
    # Select 3 diverse examples
    long_signals = [c for c in crossovers if c[1] == 'LONG']
    short_signals = [c for c in crossovers if c[1] == 'SHORT']
    
    # Example 1: LONG from early 2023
    ex1 = long_signals[5]
    
    # Example 2: SHORT from mid 2023
    ex2 = short_signals[len(short_signals)//2]
    
    # Example 3: Whipsaw (rapid reversal)
    ex3 = None
    for i in range(len(crossovers) - 1):
        if crossovers[i][1] != crossovers[i+1][1]:
            time_diff = (crossovers[i+1][0] - crossovers[i][0]).total_seconds() / 3600
            if 20 < time_diff < 80:
                ex3 = crossovers[i]
                break
    if ex3 is None:
        ex3 = long_signals[len(long_signals)//3]
    
    examples = [
        ('Example 1: LONG Signal (Golden Cross)', ex1, 'example_1_long.png'),
        ('Example 2: SHORT Signal (Death Cross)', ex2, 'example_2_short.png'),
        ('Example 3: Whipsaw (Rapid Reversal)', ex3, 'example_3_whipsaw.png'),
    ]
    
    for title, crossover_point, filename in examples:
        idx = df.index.get_loc(crossover_point[0])
        start = max(0, idx - 72)  # 72 hours before (3 days)
        end = min(len(df), idx + 24)  # 24 hours after
        
        window = df.iloc[start:end].copy()
        fast_window = ema(window['Close'].values, 12)
        slow_window = ema(window['Close'].values, 26)
        
        # Find crossover in window
        window_crossover = None
        for j in range(1, len(window)):
            if np.isnan(fast_window[j]) or np.isnan(slow_window[j]):
                continue
            if crossover_point[1] == 'LONG' and fast_window[j] > slow_window[j] and fast_window[j-1] <= slow_window[j-1]:
                window_crossover = (window.index[j], 'LONG', window['Close'].iloc[j])
                break
            elif crossover_point[1] == 'SHORT' and fast_window[j] < slow_window[j] and fast_window[j-1] >= slow_window[j-1]:
                window_crossover = (window.index[j], 'SHORT', window['Close'].iloc[j])
                break
        
        if window_crossover:
            plot_candlestick(window, fast_window, slow_window, window_crossover, title, filename)
