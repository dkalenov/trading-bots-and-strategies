import numpy as np
import talib as ta

def utbot_signal(highs, lows, closes, state, key_value=8, atr_period=10):
    """
    Function to generate trading signals based on UTBot_Strategy.

    :param highs: list of High prices
    :param lows: list of Low prices
    :param closes: list of Close prices
    :param state: dictionary to store state (x_atr_trailing_stop, pos, ema)
    :param key_value: ATR multiplier
    :param atr_period: ATR period
    :return: "BUY", "SELL", or "HOLD"
    """
    if len(closes) < atr_period + 2:
        return "HOLD"  # Not enough data for calculation

    # Calculate ATR
    atr = ta.ATR(np.array(highs), np.array(lows), np.array(closes), timeperiod=atr_period)

    # Get the latest values
    close = closes[-1]
    prev_close = closes[-2]
    n_loss = atr[-1] * key_value

    # Initialize state if not already present
    if 'x_atr_trailing_stop' not in state:
        state['x_atr_trailing_stop'] = [0]
        state['pos'] = [0]
        state['ema'] = [0]

    x_atr_trailing_stop_prev = state['x_atr_trailing_stop'][-1]

    # Calculate the new ATR trailing stop
    iff_1 = close - n_loss if close > x_atr_trailing_stop_prev else close + n_loss
    iff_2 = (min(x_atr_trailing_stop_prev, close + n_loss)
             if close < x_atr_trailing_stop_prev and prev_close < x_atr_trailing_stop_prev else iff_1)
    x_atr_trailing_stop = (max(x_atr_trailing_stop_prev, close - n_loss)
                           if close > x_atr_trailing_stop_prev and prev_close > x_atr_trailing_stop_prev
                           else iff_2)

    # Determine position
    iff_3 = (-1 if prev_close > x_atr_trailing_stop_prev and close < x_atr_trailing_stop_prev else state['pos'][-1])
    pos = 1 if prev_close < x_atr_trailing_stop_prev and close > x_atr_trailing_stop_prev else iff_3

    ema = close
    above = ema > x_atr_trailing_stop and state['ema'][-1] <= x_atr_trailing_stop_prev
    below = x_atr_trailing_stop > ema and x_atr_trailing_stop_prev <= state['ema'][-1]

    # Determine signals
    buy = close > x_atr_trailing_stop and above
    sell = close < x_atr_trailing_stop and below

    # Update state
    state['x_atr_trailing_stop'].append(x_atr_trailing_stop)
    state['pos'].append(pos)
    state['ema'].append(ema)

    if buy:
        return "BUY"
    elif sell:
        return "SELL"
    return "HOLD"
