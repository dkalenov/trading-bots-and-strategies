import numpy as np


def ADX_numpy(candles, period=14):
    candles_np = np.array(candles).astype(float)
    high = candles_np[:, 2]
    low = candles_np[:, 3]
    close = candles_np[:, 4]

    # Проверка на NaN
    if np.any(np.isnan(high)) or np.any(np.isnan(low)) or np.any(np.isnan(close)):
        print("Есть NaN в данных для ADX.")
        return np.full_like(close, np.nan)

    # Проверка длины данных
    if len(candles) < period:
        print(f"Недостаточно данных для расчета ADX. Требуется как минимум {period} свечей.")
        return np.full_like(close, np.nan)

    # TR, +DM, -DM calculations
    tr1 = high[1:] - low[1:]
    tr2 = np.abs(high[1:] - close[:-1])
    tr3 = np.abs(low[1:] - close[:-1])
    tr = np.maximum(np.maximum(tr1, tr2), tr3)

    plus_dm = np.where(high[1:] > high[:-1], np.maximum(high[1:] - high[:-1], 0), 0)
    minus_dm = np.where(low[:-1] > low[1:], np.maximum(low[:-1] - low[1:], 0), 0)

    # Smoothed calculations of TR, +DM, -DM
    tr_smooth = np.zeros_like(close)
    plus_dm_smooth = np.zeros_like(close)
    minus_dm_smooth = np.zeros_like(close)

    tr_smooth[period] = np.sum(tr[:period])
    plus_dm_smooth[period] = np.sum(plus_dm[:period])
    minus_dm_smooth[period] = np.sum(minus_dm[:period])

    for i in range(period + 1, len(close)):
        tr_smooth[i] = (tr_smooth[i - 1] * (period - 1) + tr[i - 1]) / period
        plus_dm_smooth[i] = (plus_dm_smooth[i - 1] * (period - 1) + plus_dm[i - 1]) / period
        minus_dm_smooth[i] = (minus_dm_smooth[i - 1] * (period - 1) + minus_dm[i - 1]) / period

    epsilon = 1e-10 # Добавляем epsilon для избегания деления на ноль в случае нулевого периода
    plus_di = 100 * (plus_dm_smooth / np.where(tr_smooth == 0, epsilon, tr_smooth))
    minus_di = 100 * (minus_dm_smooth / np.where(tr_smooth == 0, epsilon, tr_smooth))


    dx = 100 * np.abs((plus_di - minus_di) / np.where((plus_di + minus_di) == 0, epsilon, (plus_di + minus_di)))


    adx = np.zeros_like(close)
    adx[period] = np.mean(dx[:period])
    for i in range(period + 1, len(close)):
        adx[i] = (adx[i - 1] * (period - 1) + dx[i - 1]) / period

    return adx




def SuperTrend_numpy(candles, period=10, multiplier=3):
    candles_np = np.array(candles).astype(float)
    close = candles_np[:, 4]
    high = candles_np[:, 2]
    low = candles_np[:, 3]

    atr = np.zeros_like(close)

    prev_close = np.roll(close, 1)
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    tr = np.maximum(np.maximum(tr1, tr2), tr3)

    atr[period] = np.mean(tr[:period])
    for i in range(period + 1, len(close)):
        atr[i] = ((period - 1) * atr[i - 1] + tr[i]) / period

    upper_band = (high + low) / 2 + multiplier * atr
    lower_band = (high + low) / 2 - multiplier * atr

    final_upper_band = np.zeros_like(close)
    final_lower_band = np.zeros_like(close)

    for i in range(period, len(close)):
        final_upper_band[i] = upper_band[i] if upper_band[i] < final_upper_band[i - 1] or close[i - 1] > final_upper_band[i - 1] else final_upper_band[i - 1]
        final_lower_band[i] = lower_band[i] if lower_band[i] > final_lower_band[i - 1] or close[i - 1] < final_lower_band[i - 1] else final_lower_band[i - 1]

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

    stx = np.where((st > 0.00), np.where((close < st), 'down', 'up'), 'nan')

    return stx

