import numpy as np


def SuperTrend_numpy(candles, period=10, multiplier=3):
    """
    Computes the Supertrend indicator given the OHLC data (open, high, low, close).

    :param candles: numpy array of OHLC candles (open, high, low, close, volume)
    :param period: lookback period for computing the average true range (ATR)
    :param multiplier: factor to multiply the ATR by to determine the offset
    :return: numpy array of Supertrend values ('up', 'down')
    """

    # Convert input to a numpy array and extract OHLC data
    candles_np = np.array(candles).astype(float)
    close = candles_np[:, 4]
    high = candles_np[:, 2]
    low = candles_np[:, 3]

    # Initialize arrays for True Range (TR) and ATR (Average True Range)
    atr = np.zeros_like(close)

    # Compute True Range (TR)
    prev_close = np.roll(close, 1)
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    tr = np.maximum(np.maximum(tr1, tr2), tr3)

    # Calculate the initial ATR value using the first 'period' values
    atr[period] = np.mean(tr[:period])

    # Continue calculating ATR for the rest of the data
    for i in range(period + 1, len(close)):
        atr[i] = ((period - 1) * atr[i - 1] + tr[i]) / period

    # Calculate the upper and lower bands
    upper_band = (high + low) / 2 + multiplier * atr
    lower_band = (high + low) / 2 - multiplier * atr

    # Final bands to be adjusted according to trend conditions
    final_upper_band = np.zeros_like(close)
    final_lower_band = np.zeros_like(close)

    for i in range(period, len(close)):
        final_upper_band[i] = upper_band[i] if upper_band[i] < final_upper_band[i - 1] or close[i - 1] > \
                                               final_upper_band[i - 1] else final_upper_band[i - 1]
        final_lower_band[i] = lower_band[i] if lower_band[i] > final_lower_band[i - 1] or close[i - 1] < \
                                               final_lower_band[i - 1] else final_lower_band[i - 1]

    # Supertrend calculation
    st = np.zeros_like(close)
    for i in range(period, len(close)):
        if st[i - 1] == final_upper_band[i - 1] and close[i] <= final_upper_band[i]:
            st[i] = final_upper_band[i]
        elif st[i - 1] == final_upper_band[i - 1] and close[i] > final_upper_band[i]:
            st[i] = final_lower_band[i]
        elif st[i - 1] == final_lower_band[i - 1] and close[i] >= final_lower_band[i]:
            st[i] = final_lower_band[i]
        elif st[i - 1] == final_lower_band[i - 1] and close[i] < final_lower_band[i]:
            st[i] = final_upper_band[i]

    # Generate buy/sell signals ('up' or 'down')
    stx = np.where((st > 0.00), np.where((close < st), 'down', 'up'), 'nan')

    return stx

