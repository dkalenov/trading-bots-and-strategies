import asyncio
import configparser
import logging
import binance
import db
import talib
import pandas as pd
from collections import Counter
import get_data
import numpy as np


# 21 idicators close
# def get_technical_rating(klines):
#     df = pd.DataFrame(klines, columns=[
#         'timestamp', 'open', 'high', 'low', 'close', 'volume',
#         '_1', '_2', '_3', '_4', '_5', '_6'
#     ])
#     df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].astype(float)
#
#     close = df['close'].values
#     high = df['high'].values
#     low = df['low'].values
#     volume = df['volume'].values
#
#     signals = []
#
#     # === Moving Averages ===
#     ma_periods = [10, 20, 30, 50, 100, 200]
#     for p in ma_periods:
#         ema = talib.EMA(close, timeperiod=p)
#         sma = talib.SMA(close, timeperiod=p)
#         if close[-1] > ema[-1]:
#             signals.append('BUY')
#         elif close[-1] < ema[-1]:
#             signals.append('SELL')
#         else:
#             signals.append('NEUTRAL')
#         if close[-1] > sma[-1]:
#             signals.append('BUY')
#         elif close[-1] < sma[-1]:
#             signals.append('SELL')
#         else:
#             signals.append('NEUTRAL')
#
#     # === Oscillators ===
#     rsi = talib.RSI(close, timeperiod=14)
#     if rsi[-1] > 70:
#         signals.append('SELL')
#     elif rsi[-1] < 30:
#         signals.append('BUY')
#     else:
#         signals.append('NEUTRAL')
#
#     macd, macdsignal, _ = talib.MACD(close)
#     if macd[-1] > macdsignal[-1]:
#         signals.append('BUY')
#     elif macd[-1] < macdsignal[-1]:
#         signals.append('SELL')
#     else:
#         signals.append('NEUTRAL')
#
#     slowk, slowd = talib.STOCH(high, low, close)
#     if slowk[-1] > 80:
#         signals.append('SELL')
#     elif slowk[-1] < 20:
#         signals.append('BUY')
#     else:
#         signals.append('NEUTRAL')
#
#     stochrsi_k, _ = talib.STOCHRSI(close)
#     if stochrsi_k[-1] > 80:
#         signals.append('SELL')
#     elif stochrsi_k[-1] < 20:
#         signals.append('BUY')
#     else:
#         signals.append('NEUTRAL')
#
#     willr = talib.WILLR(high, low, close)
#     if willr[-1] < -80:
#         signals.append('BUY')
#     elif willr[-1] > -20:
#         signals.append('SELL')
#     else:
#         signals.append('NEUTRAL')
#
#     cci = talib.CCI(high, low, close)
#     if cci[-1] > 100:
#         signals.append('BUY')
#     elif cci[-1] < -100:
#         signals.append('SELL')
#     else:
#         signals.append('NEUTRAL')
#
#     adx = talib.ADX(high, low, close)
#     if adx[-1] > 25:
#         signals.append('BUY')
#     else:
#         signals.append('NEUTRAL')
#
#     mom = talib.MOM(close, timeperiod=10)
#     if mom[-1] > 0:
#         signals.append('BUY')
#     elif mom[-1] < 0:
#         signals.append('SELL')
#     else:
#         signals.append('NEUTRAL')
#
#     ultosc = talib.ULTOSC(high, low, close)
#     if ultosc[-1] > 70:
#         signals.append('SELL')
#     elif ultosc[-1] < 30:
#         signals.append('BUY')
#     else:
#         signals.append('NEUTRAL')
#
#     # === Подсчёт ===
#     counter = Counter(signals)
#     total = len(signals)
#
#     buy_ratio = counter['BUY'] / total
#     sell_ratio = counter['SELL'] / total
#
#     print(f"BUY: {counter['BUY']}, SELL: {counter['SELL']}, NEUTRAL: {counter['NEUTRAL']}")
#
#     if buy_ratio >= 0.66:
#         return 'STRONG_BUY'
#     elif sell_ratio >= 0.66:
#         return 'STRONG_SELL'
#     elif buy_ratio > sell_ratio:
#         return 'BUY'
#     elif sell_ratio > buy_ratio:
#         return 'SELL'
#     else:
#         return 'NEUTRAL'






# 25 indicators
# def get_technical_rating(klines):
#     df = pd.DataFrame(klines, columns=[
#         'timestamp', 'open', 'high', 'low', 'close', 'volume',
#         '_1', '_2', '_3', '_4', '_5', '_6']
#     )
#     df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].astype(float)
#
#     close = df['close'].values
#     high = df['high'].values
#     low = df['low'].values
#     volume = df['volume'].values
#
#     signals = []
#     ma_signals = []
#     osc_signals = []
#
#     # Moving Averages (SMA, EMA)
#     for p in [10, 20, 30, 50, 100, 200]:
#         for ma_func in [talib.SMA, talib.EMA]:
#             ma = ma_func(close, timeperiod=p)
#             if close[-1] > ma[-1]:
#                 ma_signals.append('BUY')
#             elif close[-1] < ma[-1]:
#                 ma_signals.append('SELL')
#             else:
#                 ma_signals.append('NEUTRAL')
#
#     # VWMA (Volume Weighted MA) manually
#     for p in [20]:
#         vwap = np.sum(close[-p:] * volume[-p:]) / np.sum(volume[-p:])
#         if close[-1] > vwap:
#             ma_signals.append('BUY')
#         elif close[-1] < vwap:
#             ma_signals.append('SELL')
#         else:
#             ma_signals.append('NEUTRAL')
#
#     # Oscillators
#     rsi = talib.RSI(close, timeperiod=14)
#     osc_signals.append('SELL' if rsi[-1] > 70 else 'BUY' if rsi[-1] < 30 else 'NEUTRAL')
#
#     macd, macdsignal, _ = talib.MACD(close)
#     osc_signals.append('BUY' if macd[-1] > macdsignal[-1] else 'SELL' if macd[-1] < macdsignal[-1] else 'NEUTRAL')
#
#     slowk, slowd = talib.STOCH(high, low, close)
#     osc_signals.append('SELL' if slowk[-1] > 80 else 'BUY' if slowk[-1] < 20 else 'NEUTRAL')
#
#     stochrsi_k, _ = talib.STOCHRSI(close, timeperiod=14)
#     osc_signals.append('SELL' if stochrsi_k[-1] > 80 else 'BUY' if stochrsi_k[-1] < 20 else 'NEUTRAL')
#
#     willr = talib.WILLR(high, low, close)
#     osc_signals.append('BUY' if willr[-1] < -80 else 'SELL' if willr[-1] > -20 else 'NEUTRAL')
#
#     cci = talib.CCI(high, low, close, timeperiod=20)
#     osc_signals.append('BUY' if cci[-1] < -100 else 'SELL' if cci[-1] > 100 else 'NEUTRAL')
#
#     adx = talib.ADX(high, low, close)
#     osc_signals.append('BUY' if adx[-1] > 25 else 'NEUTRAL')
#
#     mom = talib.MOM(close, timeperiod=10)
#     osc_signals.append('BUY' if mom[-1] > 0 else 'SELL' if mom[-1] < 0 else 'NEUTRAL')
#
#     ultosc = talib.ULTOSC(high, low, close)
#     osc_signals.append('SELL' if ultosc[-1] > 70 else 'BUY' if ultosc[-1] < 30 else 'NEUTRAL')
#
#     # AO (Awesome Oscillator)
#     median_price = (high + low) / 2
#     ao = talib.SMA(median_price, timeperiod=5) - talib.SMA(median_price, timeperiod=34)
#     osc_signals.append('BUY' if ao[-1] > 0 else 'SELL' if ao[-1] < 0 else 'NEUTRAL')
#
#     # Bulls Power & Bears Power
#     ema13 = talib.EMA(close, timeperiod=13)
#     bulls = high - ema13
#     bears = low - ema13
#     osc_signals.append('BUY' if bulls[-1] > 0 else 'SELL')
#     osc_signals.append('SELL' if bears[-1] < 0 else 'BUY')
#
#     # Combine
#     signals = ma_signals + osc_signals
#     counter = Counter(signals)
#     total = len(signals)
#
#     buy_ratio = counter['BUY'] / total
#     sell_ratio = counter['SELL'] / total
#
#     print(f"BUY: {counter['BUY']}, SELL: {counter['SELL']}, NEUTRAL: {counter['NEUTRAL']}")
#
#     if buy_ratio >= 0.66:
#         return 'STRONG_BUY'
#     elif sell_ratio >= 0.66:
#         return 'STRONG_SELL'
#     elif buy_ratio > sell_ratio:
#         return 'BUY'
#     elif sell_ratio > buy_ratio:
#         return 'SELL'
#     else:
#         return 'NEUTRAL'





# # 29 indicators

def get_technical_rating(klines):
    df = pd.DataFrame(klines, columns=['timestamp','open','high','low','close','volume']+['_']*6).astype(float)

    # 1) Heikin Ashi преобразование (альтернативные цены для расчётов)
    ha_open = (df['open'] + df['close']) / 2
    ha_close = (df[['open','high','low','close']].sum(axis=1)) / 4
    ha_high = df[['high', 'open', 'close']].max(axis=1)
    ha_low = df[['low', 'open', 'close']].min(axis=1)

    close, high, low, vol = ha_close.values, ha_high.values, ha_low.values, df['volume'].values

    ma_sigs, osc_sigs = [], []

    # === MA: SMA/EMA (10,20,30,50,100,200), VWMA20, HullMA9 ===
    for p in [10,20,30,50,100,200]:
        for fn in [talib.SMA, talib.EMA]:
            m = fn(close, timeperiod=p)
            ma_sigs.append('BUY' if close[-1] > m[-1] else 'SELL' if close[-1] < m[-1] else 'NEUTRAL')

    # VWMA (20)
    p = 20
    vwma = (close[-p:]*vol[-p:]).sum()/vol[-p:].sum()
    ma_sigs.append('BUY' if close[-1] > vwma else 'SELL' if close[-1] < vwma else 'NEUTRAL')

    # Hull MA (9)
    wma_half = talib.WMA(close, timeperiod=int(9/2)+1)
    wma_full = talib.WMA(close, timeperiod=9)
    hull = talib.WMA(2*wma_half - wma_full, timeperiod=int(np.sqrt(9)))
    ma_sigs.append('BUY' if close[-1] > hull[-1] else 'SELL' if close[-1] < hull[-1] else 'NEUTRAL')

    # === Ichimoku: Tenkan/Kijun + SenkouA/B ===
    conv = (df['high'].rolling(9).max() + df['low'].rolling(9).min()) / 2
    base = (df['high'].rolling(26).max() + df['low'].rolling(26).min()) / 2
    spanA = ((conv + base) / 2).shift(26)
    spanB = ((df['high'].rolling(52).max() + df['low'].rolling(52).min()) / 2).shift(26)
    ich_buy = int(conv.iloc[-1] > base.iloc[-1]) + int(spanA.iloc[-27] > spanB.iloc[-27])
    ich_sell = int(conv.iloc[-1] < base.iloc[-1]) + int(spanA.iloc[-27] < spanB.iloc[-27])
    if ich_buy > ich_sell: ma_sigs.append('BUY')
    elif ich_sell > ich_buy: ma_sigs.append('SELL')
    else: ma_sigs.append('NEUTRAL')

    # === Oscillators ===
    # RSI(14)
    rsi = talib.RSI(close, timeperiod=14)
    osc_sigs.append('SELL' if rsi[-1] > 70 else 'BUY' if rsi[-1] < 30 else 'NEUTRAL')

    # Stochastic %K %D
    k, d = talib.STOCH(high, low, close, fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    osc_sigs.append('SELL' if k[-1] > 80 else 'BUY' if k[-1] < 20 else 'NEUTRAL')

    # StochRSI(14)
    stochrsi_k, stochrsi_d = talib.STOCHRSI(close, timeperiod=14, fastk_period=3, fastd_period=3)
    osc_sigs.append('SELL' if stochrsi_k[-1] > 80 else 'BUY' if stochrsi_k[-1] < 20 else 'NEUTRAL')

    # MACD
    m, ms, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    osc_sigs.append('BUY' if m[-1] > ms[-1] else 'SELL' if m[-1] < ms[-1] else 'NEUTRAL')

    # Awesome Oscillator
    mp = (high + low) / 2
    ao = talib.SMA(mp,5) - talib.SMA(mp,34)
    osc_sigs.append('BUY' if ao[-1] > 0 else 'SELL' if ao[-1] < 0 else 'NEUTRAL')

    # Momentum(10)
    mom = talib.MOM(close, timeperiod=10)
    osc_sigs.append('BUY' if mom[-1] > 0 else 'SELL' if mom[-1] < 0 else 'NEUTRAL')

    # Williams %R
    willr = talib.WILLR(high, low, close, timeperiod=14)
    osc_sigs.append('SELL' if willr[-1] > -20 else 'BUY' if willr[-1] < -80 else 'NEUTRAL')

    # CCI(20)
    cci = talib.CCI(high, low, close, timeperiod=20)
    osc_sigs.append('SELL' if cci[-1] > 100 else 'BUY' if cci[-1] < -100 else 'NEUTRAL')

    # ADX & DI
    adx = talib.ADX(high, low, close, timeperiod=14)
    plus_di = talib.PLUS_DI(high, low, close, timeperiod=14)
    minus_di = talib.MINUS_DI(high, low, close, timeperiod=14)
    osc_sigs.append('BUY' if plus_di[-1] > minus_di[-1] and adx[-1] > 25 else 'SELL' if minus_di[-1] > plus_di[-1] and adx[-1] > 25 else 'NEUTRAL')

    # Bulls/Bears Power
    ema13 = talib.EMA(close, timeperiod=13)
    bulls = high[-1] - ema13[-1]
    bears = low[-1] - ema13[-1]
    osc_sigs.append('BUY' if bulls > 0 else 'NEUTRAL')
    osc_sigs.append('SELL' if bears < 0 else 'NEUTRAL')

    # Ultimate Oscillator
    ult = talib.ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
    osc_sigs.append('SELL' if ult[-1] > 70 else 'BUY' if ult[-1] < 30 else 'NEUTRAL')

    # Bollinger Bands %B
    upper, mid, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
    bbp = (close[-1] - lower[-1]) / (upper[-1] - lower[-1])
    osc_sigs.append('SELL' if bbp > 1 else 'BUY' if bbp < 0 else 'NEUTRAL')

    # Pivot Points Classic
    pivot = (high[-2] + low[-2] + close[-2]) / 3
    osc_sigs.append('BUY' if close[-1] > pivot else 'SELL' if close[-1] < pivot else 'NEUTRAL')

    # ===== Aggregate & final rating =====
    signals = ma_sigs + osc_sigs
    cnt = Counter(signals)
    total = len(signals)
    print(f"BUY:{cnt['BUY']} SELL:{cnt['SELL']} NEUTRAL:{cnt['NEUTRAL']} (total={total})")

    br, sr = cnt['BUY']/total, cnt['SELL']/total
    if br >= 0.66: return 'STRONG_BUY'
    if sr >= 0.66: return 'STRONG_SELL'
    if cnt['BUY'] > cnt['SELL']: return 'BUY'
    if cnt['SELL'] > cnt['BUY']: return 'SELL'
    return 'NEUTRAL'





# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures

symbols = ['BTCUSDT', 'ETHUSDT']

interval = '5m'



# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
config = configparser.ConfigParser()

async def main():
    config.read('config.ini')
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                               config['DB']['password'], config['DB']['db'])

    conf = await db.load_config()
    client = binance.Futures(
        conf.api_key, conf.api_secret,
        asynced=True, testnet=config.getboolean('BOT', 'testnet')
    )
    debug = config.getboolean('BOT', 'debug')

    for symbol in symbols:
        try:
            klines = await client.klines(symbol, interval=interval, limit=150)
            rating = get_technical_rating(klines)
            print(f"{symbol} rating: {rating}")


            tradingview_data = get_data.get_tradingview_data(symbol, interval)
            print(tradingview_data)
        except Exception as e:
            logging.error(f"Ошибка при получении свечей для {symbol}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())



