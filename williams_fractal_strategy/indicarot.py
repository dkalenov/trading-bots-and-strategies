import pandas as pd
import numpy as np
from main import save_candlestick_plot


symbol = 'BTCUSDT'
interval = '1h'
start_date = '2024-11'
end_date = '2025-02'


file_path = f'klines_{symbol}_{interval}_{start_date}_{end_date}.csv'
df = pd.read_csv(file_path)

df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

df = df.copy()

df["max1"] = np.nan
df["min1"] = np.nan
df["max2"] = np.nan
df["min2"] = np.nan
df["max1_date"] = np.nan
df["min1_date"] = np.nan
df["max2_date"] = np.nan
df["min2_date"] = np.nan
df["signal"] = 0  # 1 = Long, -1 = Short


last_max1 = last_min1 = last_max2 = last_min2 = None
date_last_max1 = date_last_min1 = date_last_max2 = date_last_min2 = None


for i in range(2, len(df) - 2):
    current_high = df.iloc[i]['high']
    current_low = df.iloc[i]['low']
    current_date = df.iloc[i]['date']

    # Check for upper fractal
    if current_high > df.iloc[i - 1]['high'] and current_high > df.iloc[i + 1]['high'] \
            and current_high > df.iloc[i - 2]['high'] and current_high > df.iloc[i + 2]['high']:
        if last_max1 is None:
            last_max1, date_last_max1 = current_high, current_date
            df.loc[df.index[i], 'max1'] = last_max1
            df.loc[df.index[i], 'max1_date'] = date_last_max1
        elif last_max2 is None and last_min1 is not None:
            last_max2, date_last_max2 = current_high, current_date
            df.loc[df.index[i], 'max2'] = last_max2
            df.loc[df.index[i], 'max2_date'] = date_last_max2

            # Reset if max2 is higher than max1
            if last_max2 > last_max1:
                last_max1 = last_min1 = last_max2 = None
                date_last_max1 = date_last_min1 = date_last_max2 = None
            elif last_max2 < last_min1:
                last_max1, date_last_max1 = last_max2, date_last_max2
                last_min1 = last_max2 = None
                date_last_min1 = date_last_max2 = None

    # Check for lower fractal
    if current_low < df.iloc[i - 1]['low'] and current_low < df.iloc[i + 1]['low'] \
            and current_low < df.iloc[i - 2]['low'] and current_low < df.iloc[i + 2]['low']:
        if last_min1 is None and last_max1 is not None:
            last_min1, date_last_min1 = current_low, current_date
            df.loc[df.index[i], 'min1'] = last_min1
            df.loc[df.index[i], 'min1_date'] = date_last_min1
        elif last_min2 is None and last_max2 is not None:
            last_min2, date_last_min2 = current_low, current_date
            df.loc[df.index[i], 'min2'] = last_min2
            df.loc[df.index[i], 'min2_date'] = date_last_min2

            if last_min2 is not None and last_min1 is not None:
                # Reset if min2 is higher than min1
                if last_min2 > last_min1:
                    last_max1 = last_min1 = last_max2 = last_min2 = None
                    date_last_max1 = date_last_min1 = date_last_max2 = date_last_min2 = None
                elif last_min2 > last_max1:
                    last_min1, date_last_min1 = last_min2, date_last_min2
                    last_max1 = last_min2 = None
                    date_last_max1 = date_last_min2 = None

    # Long signal: breakout of the last minimum
    if last_max1 is not None and last_min1 is not None and last_max2 is not None:
        if current_low < last_min1:
            df.loc[df.index[i], 'signal'] = 1  # Long
            df.loc[df.index[i], ['min1', 'max1', 'max2', 'min1_date', 'max1_date', 'max2_date']] = \
                [last_min1, last_max1, last_max2, date_last_min1, date_last_max1, date_last_max2]
            last_max1 = last_min1 = last_max2 = None

    # Short signal: breakout of the last maximum
    if last_min1 is not None and last_max1 is not None and last_min2 is not None:
        if current_high > last_max1:
            df.loc[df.index[i], 'signal'] = -1  # Short
            df.loc[df.index[i], ['min1', 'max1', 'min2', 'min1_date', 'max1_date', 'min2_date']] = \
                [last_min1, last_max1, last_min2, date_last_min1, date_last_max1, date_last_min2]
            last_min1 = last_max1 = last_min2 = None


df.to_csv(f'fractal_signals_{symbol}_{interval}_{start_date}_{end_date}.csv', index=False)

# df = pd.read_csv(f'fractal_signals_{symbol}_{interval}_{start_date}_{end_date}.csv', parse_dates=['date'], index_col='date')
# df.index = pd.to_datetime(df.index, utc=True)
#
# hours_window = 48
# save_candlestick_plot(df, symbol, interval, hours_window)
