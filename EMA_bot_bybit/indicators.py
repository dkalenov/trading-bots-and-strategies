import numpy as np

def ema(data, period):
    ema_values = np.zeros_like(data)
    sma = np.mean(data[:period])
    ema_values[period - 1] = sma
    multiplier = 2 / (period + 1) # smoothing coefficient
    for i in range(period, len(data)):
        ema_values[i] = (data[i] - ema_values[i - 1]) * multiplier + ema_values[i - 1]
    
    return ema_values