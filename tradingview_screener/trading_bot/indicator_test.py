# from tradingview_ta import TA_Handler, Interval
# import asyncio
# import logging
# from datetime import datetime, timezone, timedelta
#
# async def wait_for_next_candle(timeframe: str):
#     now = datetime.now(timezone.utc)
#     if "m" in timeframe:
#         tf_minutes = int(timeframe[:-1])
#         next_minute = (now.minute // tf_minutes + 1) * tf_minutes
#         next_time = now.replace(second=0, microsecond=0) + timedelta(minutes=(next_minute - now.minute))
#     else:
#         raise ValueError(f"Неподдерживаемый таймфрейм: {timeframe}")
#
#     wait_seconds = max((next_time - now).total_seconds(), 0)
#     logging.info(f"[{timeframe}] Ждем {int(wait_seconds)} секунд до следующей свечи")
#     await asyncio.sleep(wait_seconds)
#
# async def fetch_analysis(handler):
#     while True:
#         await wait_for_next_candle("5m")  # Ждем до следующей минуты
#         print(datetime.now(), handler.get_analysis().summary)
#
# async def main():
#     symbol = "BTCUSDT"
#     handler = TA_Handler(
#         symbol=symbol,
#         exchange="BINANCE",
#         screener="crypto",
#         interval=Interval.INTERVAL_5_MINUTES
#     )
#
#     await fetch_analysis(handler)
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     asyncio.run(main())


# symbols = ['BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'LTCUSDT', 'TRXUSDT', 'ETCUSDT', 'LINKUSDT', 'XLMUSDT', 'ADAUSDT', 'DASHUSDT', 'ZECUSDT', 'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT', 'IOTAUSDT', 'BATUSDT', 'VETUSDT', 'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT', 'KNCUSDT', 'ZRXUSDT', 'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'KAVAUSDT', 'BANDUSDT', 'RLCUSDT', 'MKRUSDT', 'SNXUSDT', 'DOTUSDT', 'DEFIUSDT', 'YFIUSDT', 'BALUSDT', 'CRVUSDT', 'TRBUSDT', 'RUNEUSDT', 'SUSHIUSDT', 'EGLDUSDT', 'SOLUSDT', 'ICXUSDT', 'STORJUSDT', 'UNIUSDT', 'AVAXUSDT', 'ENJUSDT', 'FLMUSDT', 'KSMUSDT', 'NEARUSDT', 'AAVEUSDT', 'FILUSDT', 'RSRUSDT', 'LRCUSDT', 'BELUSDT', 'AXSUSDT', 'ALPHAUSDT', 'ZENUSDT', 'SKLUSDT', 'GRTUSDT', 'BNTUSDT', '1INCHUSDT', 'CHZUSDT', 'SANDUSDT', 'ANKRUSDT', 'LITUSDT', 'RVNUSDT', 'SFPUSDT', 'XEMUSDT', 'COTIUSDT', 'CHRUSDT', 'MANAUSDT', 'ALICEUSDT', 'HBARUSDT', 'ONEUSDT', 'LINAUSDT', 'STMXUSDT', 'DENTUSDT', 'CELRUSDT', 'HOTUSDT', 'MTLUSDT', 'OGNUSDT', 'NKNUSDT', '1000SHIBUSDT', 'BAKEUSDT', 'GTCUSDT', 'BTCDOMUSDT', 'MASKUSDT', 'IOTXUSDT', 'C98USDT', 'ATAUSDT', 'DYDXUSDT', '1000XECUSDT', 'GALAUSDT', 'CELOUSDT', 'ARUSDT', 'ARPAUSDT', 'CTSIUSDT', 'LPTUSDT', 'ENSUSDT', 'PEOPLEUSDT', 'ROSEUSDT', 'DUSKUSDT', 'FLOWUSDT', 'IMXUSDT', 'API3USDT', 'GMTUSDT', 'APEUSDT', 'WOOUSDT', 'JASMYUSDT', 'OPUSDT', 'XMRUSDT', 'INJUSDT', 'STGUSDT', 'SPELLUSDT', '1000LUNCUSDT', 'LUNA2USDT', 'LDOUSDT', 'APTUSDT', 'QNTUSDT', 'FXSUSDT', 'HOOKUSDT', 'MAGICUSDT', 'TUSDT', 'HIGHUSDT', 'MINAUSDT', 'ASTRUSDT', 'PHBUSDT', 'FETUSDT', 'GMXUSDT', 'CFXUSDT', 'STXUSDT', 'ACHUSDT', 'SSVUSDT', 'CKBUSDT', 'PERPUSDT', 'LQTYUSDT', 'ARBUSDT', 'IDUSDT', 'JOEUSDT', 'LEVERUSDT', 'TRUUSDT', 'RDNTUSDT', 'HFTUSDT', 'XVSUSDT', 'BLURUSDT', 'EDUUSDT', '1000PEPEUSDT', '1000FLOKIUSDT', 'UMAUSDT', 'COMBOUSDT', 'SUIUSDT', 'NMRUSDT', 'MAVUSDT', 'XVGUSDT', 'WLDUSDT', 'PENDLEUSDT', 'ARKMUSDT', 'AGLDUSDT', 'YGGUSDT', 'DODOXUSDT', 'OXTUSDT', 'SEIUSDT', 'CYBERUSDT', 'HIFIUSDT', 'ARKUSDT', 'BICOUSDT', 'LOOMUSDT', 'BIGTIMEUSDT', 'BONDUSDT', 'ORBSUSDT', 'WAXPUSDT', 'BSVUSDT', 'RIFUSDT', 'POLYXUSDT', 'GASUSDT', 'POWRUSDT', 'TIAUSDT', 'CAKEUSDT', 'MEMEUSDT', 'TOKENUSDT', 'ORDIUSDT', 'STEEMUSDT', 'ILVUSDT', 'NTRNUSDT', 'KASUSDT', 'BEAMXUSDT', '1000BONKUSDT', 'PYTHUSDT', 'SUPERUSDT', 'USTCUSDT', 'ONGUSDT', 'ETHWUSDT', 'JTOUSDT', '1000SATSUSDT', 'AUCTIONUSDT', '1000RATSUSDT', 'ACEUSDT', 'MOVRUSDT', 'TWTUSDT', 'NFPUSDT', 'AIUSDT', 'XAIUSDT', 'WIFUSDT', 'MANTAUSDT', 'ONDOUSDT', 'LSKUSDT', 'ALTUSDT', 'JUPUSDT', 'ZETAUSDT', 'RONINUSDT', 'DYMUSDT', 'OMUSDT', 'PIXELUSDT', 'STRKUSDT', 'GLMUSDT', 'PORTALUSDT', 'TONUSDT', 'AXLUSDT', 'MYROUSDT', 'METISUSDT', 'AEVOUSDT', 'VANRYUSDT', 'BOMEUSDT', 'ETHFIUSDT', 'ENAUSDT', 'WUSDT', 'TNSRUSDT', 'SAGAUSDT', 'TAOUSDT', 'OMNIUSDT', 'REZUSDT', 'BBUSDT', 'NOTUSDT', 'TURBOUSDT', 'IOUSDT', 'ZKUSDT', 'MEWUSDT', 'LISTAUSDT', 'ZROUSDT', 'RENDERUSDT', 'BANANAUSDT', 'RAREUSDT', 'GUSDT', 'SYNUSDT', 'SYSUSDT', 'VOXELUSDT', 'BRETTUSDT', 'ALPACAUSDT', 'POPCATUSDT', 'SUNUSDT', 'VIDTUSDT', 'NULSUSDT', 'DOGSUSDT', 'MBOXUSDT', 'CHESSUSDT', 'FLUXUSDT', 'BSWUSDT', 'QUICKUSDT', 'NEIROETHUSDT', 'RPLUSDT', 'POLUSDT', 'UXLINKUSDT', 'NEIROUSDT', '1MBABYDOGEUSDT', 'KDAUSDT', 'FIDAUSDT', 'FIOUSDT', 'CATIUSDT', 'GHSTUSDT', 'LOKAUSDT', 'HMSTRUSDT', 'REIUSDT', 'COSUSDT', 'EIGENUSDT', 'DIAUSDT', '1000CATUSDT', 'SCRUSDT', 'GOATUSDT', 'MOODENGUSDT', 'SAFEUSDT', 'SANTOSUSDT', 'TROYUSDT', 'PONKEUSDT', 'CETUSUSDT', 'COWUSDT', '1000000MOGUSDT', 'GRASSUSDT', 'DRIFTUSDT', 'SWELLUSDT', 'PNUTUSDT', 'ACTUSDT', 'HIPPOUSDT', '1000XUSDT', 'DEGENUSDT', 'BANUSDT', 'AKTUSDT', 'SLERFUSDT', 'SCRTUSDT', '1000WHYUSDT', '1000CHEEMSUSDT', 'THEUSDT', 'MORPHOUSDT', 'CHILLGUYUSDT', 'KAIAUSDT', 'AEROUSDT', 'ACXUSDT', 'ORCAUSDT', 'MOVEUSDT', 'RAYSOLUSDT', 'VIRTUALUSDT', 'SPXUSDT', 'KOMAUSDT', 'MEUSDT', 'ABCCCUSDT', 'AVAUSDT', 'DEGOUSDT', 'VELODROMEUSDT', 'MOCAUSDT', 'VANAUSDT', 'PENGUUSDT', 'LUMIAUSDT', 'USUALUSDT', 'KMNOUSDT', 'CGPTUSDT', 'AIXBTUSDT', 'FARTCOINUSDT', 'HIVEUSDT', 'DEXEUSDT', 'PHAUSDT', 'DFUSDT', 'GRIFFAINUSDT', 'AI16ZUSDT', 'ZEREBROUSDT', 'BIOUSDT', 'COOKIEUSDT', 'ALCHUSDT', 'SWARMSUSDT', 'SONICUSDT', 'DUSDT', 'PROMUSDT', 'SUSDT', 'ARCUSDT', 'AVAAIUSDT', 'TRUMPUSDT', 'MELANIAUSDT', 'VTHOUSDT', 'ANIMEUSDT', 'PIPPINUSDT', 'VINEUSDT', 'VVVUSDT', 'BERAUSDT', 'TSTUSDT', 'LAYERUSDT', 'HEIUSDT', 'B3USDT', 'IPUSDT', 'GPSUSDT', 'SHELLUSDT', 'KAITOUSDT', 'REDUSDT', 'VICUSDT', 'BMTUSDT', 'MUBARAKUSDT', 'BROCCOLI714USDT', 'BROCCOLIF3BUSDT', 'BANANAS31USDT', 'SIRENUSDT', 'BNXUSDT', 'BRUSDT', 'PLUMEUSDT', 'PAXGUSDT', 'WALUSDT', 'FUNUSDT', 'MLNUSDT', 'ATHUSDT', 'BABYUSDT', 'FORTHUSDT', 'PROMPTUSDT', 'XCNUSDT', 'PUMPUSDT', 'STOUSDT', 'FHEUSDT', 'INITUSDT', 'BANKUSDT', 'EPTUSDT', 'DEEPUSDT', 'HYPERUSDT', 'MEMEFIUSDT', 'FISUSDT', 'JSTUSDT', 'SIGNUSDT', 'PUNDIXUSDT', 'AIOTUSDT', 'DOLOUSDT', 'HAEDALUSDT', 'SXTUSDT', 'ALPINEUSDT', 'ASRUSDT', 'B2USDT', 'MILKUSDT', 'SYRUPUSDT', 'OBOLUSDT', 'OGUSDT', 'ZKJUSDT', 'SKYAIUSDT', 'NXPCUSDT', 'AGTUSDT', 'BUSDT', 'AUSDT', 'MERLUSDT', 'HYPEUSDT', 'BDXNUSDT', 'PUFFERUSDT', 'PORT3USDT', '1000000BOBUSDT', 'SKATEUSDT', 'TAIKOUSDT', 'SQDUSDT']
#

import asyncio
import configparser
import logging
import binance
import db
import talib
import pandas as pd
from collections import Counter
import get_data
from collections import Counter
import numpy as np
import datetime
from datetime import datetime, timedelta
import pytz
from datetime import timezone



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
#     # print(f"BUY: {counter['BUY']}, SELL: {counter['SELL']}, NEUTRAL: {counter['NEUTRAL']}")
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



# 29 indicators

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





def save_signals_to_csv(filename, data_rows):
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['symbol', 'interval', 'signal', 'entry_price', 'utc_time'])

        if not file_exists:
            writer.writeheader()

        writer.writerows(data_rows)



import os
import csv

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures

symbols = ['BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'LTCUSDT', 'TRXUSDT', 'ETCUSDT', 'LINKUSDT', 'XLMUSDT', 'ADAUSDT', 'DASHUSDT', 'ZECUSDT', 'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT', 'IOTAUSDT', 'BATUSDT', 'VETUSDT', 'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT', 'KNCUSDT', 'ZRXUSDT', 'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'KAVAUSDT', 'BANDUSDT', 'RLCUSDT', 'MKRUSDT', 'SNXUSDT', 'DOTUSDT', 'DEFIUSDT', 'YFIUSDT', 'BALUSDT', 'CRVUSDT', 'TRBUSDT', 'RUNEUSDT', 'SUSHIUSDT', 'EGLDUSDT', 'SOLUSDT', 'ICXUSDT', 'STORJUSDT', 'UNIUSDT', 'AVAXUSDT', 'ENJUSDT', 'FLMUSDT', 'KSMUSDT', 'NEARUSDT', 'AAVEUSDT', 'FILUSDT', 'RSRUSDT', 'LRCUSDT', 'BELUSDT', 'AXSUSDT', 'ALPHAUSDT', 'ZENUSDT', 'SKLUSDT', 'GRTUSDT', 'BNTUSDT', '1INCHUSDT', 'CHZUSDT', 'SANDUSDT', 'ANKRUSDT', 'LITUSDT', 'RVNUSDT', 'SFPUSDT', 'XEMUSDT', 'COTIUSDT', 'CHRUSDT', 'MANAUSDT', 'ALICEUSDT', 'HBARUSDT', 'ONEUSDT', 'LINAUSDT', 'STMXUSDT', 'DENTUSDT', 'CELRUSDT', 'HOTUSDT', 'MTLUSDT', 'OGNUSDT', 'NKNUSDT', '1000SHIBUSDT', 'BAKEUSDT', 'GTCUSDT', 'BTCDOMUSDT', 'MASKUSDT', 'IOTXUSDT', 'C98USDT', 'ATAUSDT', 'DYDXUSDT', '1000XECUSDT', 'GALAUSDT', 'CELOUSDT', 'ARUSDT', 'ARPAUSDT', 'CTSIUSDT', 'LPTUSDT', 'ENSUSDT', 'PEOPLEUSDT', 'ROSEUSDT', 'DUSKUSDT', 'FLOWUSDT', 'IMXUSDT', 'API3USDT', 'GMTUSDT', 'APEUSDT', 'WOOUSDT', 'JASMYUSDT', 'OPUSDT', 'XMRUSDT', 'INJUSDT', 'STGUSDT', 'SPELLUSDT', '1000LUNCUSDT', 'LUNA2USDT', 'LDOUSDT', 'APTUSDT', 'QNTUSDT', 'FXSUSDT', 'HOOKUSDT', 'MAGICUSDT', 'TUSDT', 'HIGHUSDT', 'MINAUSDT', 'ASTRUSDT', 'PHBUSDT', 'FETUSDT', 'GMXUSDT', 'CFXUSDT', 'STXUSDT', 'ACHUSDT', 'SSVUSDT', 'CKBUSDT', 'PERPUSDT', 'LQTYUSDT', 'ARBUSDT', 'IDUSDT', 'JOEUSDT', 'LEVERUSDT', 'TRUUSDT', 'RDNTUSDT', 'HFTUSDT', 'XVSUSDT', 'BLURUSDT', 'EDUUSDT', '1000PEPEUSDT', '1000FLOKIUSDT', 'UMAUSDT', 'COMBOUSDT', 'SUIUSDT', 'NMRUSDT', 'MAVUSDT', 'XVGUSDT', 'WLDUSDT', 'PENDLEUSDT', 'ARKMUSDT', 'AGLDUSDT', 'YGGUSDT', 'DODOXUSDT', 'OXTUSDT', 'SEIUSDT', 'CYBERUSDT', 'HIFIUSDT', 'ARKUSDT', 'BICOUSDT', 'LOOMUSDT', 'BIGTIMEUSDT', 'BONDUSDT', 'ORBSUSDT', 'WAXPUSDT', 'BSVUSDT', 'RIFUSDT', 'POLYXUSDT', 'GASUSDT', 'POWRUSDT', 'TIAUSDT', 'CAKEUSDT', 'MEMEUSDT', 'TOKENUSDT', 'ORDIUSDT', 'STEEMUSDT', 'ILVUSDT', 'NTRNUSDT', 'KASUSDT', 'BEAMXUSDT', '1000BONKUSDT', 'PYTHUSDT', 'SUPERUSDT', 'USTCUSDT', 'ONGUSDT', 'ETHWUSDT', 'JTOUSDT', '1000SATSUSDT', 'AUCTIONUSDT', '1000RATSUSDT', 'ACEUSDT', 'MOVRUSDT', 'TWTUSDT', 'NFPUSDT', 'AIUSDT', 'XAIUSDT', 'WIFUSDT', 'MANTAUSDT', 'ONDOUSDT', 'LSKUSDT', 'ALTUSDT', 'JUPUSDT', 'ZETAUSDT', 'RONINUSDT', 'DYMUSDT', 'OMUSDT', 'PIXELUSDT', 'STRKUSDT', 'GLMUSDT', 'PORTALUSDT', 'TONUSDT', 'AXLUSDT', 'MYROUSDT', 'METISUSDT', 'AEVOUSDT', 'VANRYUSDT', 'BOMEUSDT', 'ETHFIUSDT', 'ENAUSDT', 'WUSDT', 'TNSRUSDT', 'SAGAUSDT', 'TAOUSDT', 'OMNIUSDT', 'REZUSDT', 'BBUSDT', 'NOTUSDT', 'TURBOUSDT', 'IOUSDT', 'ZKUSDT', 'MEWUSDT', 'LISTAUSDT', 'ZROUSDT', 'RENDERUSDT', 'BANANAUSDT', 'RAREUSDT', 'GUSDT', 'SYNUSDT', 'SYSUSDT', 'VOXELUSDT', 'BRETTUSDT', 'ALPACAUSDT', 'POPCATUSDT', 'SUNUSDT', 'VIDTUSDT', 'NULSUSDT', 'DOGSUSDT', 'MBOXUSDT', 'CHESSUSDT', 'FLUXUSDT', 'BSWUSDT', 'QUICKUSDT', 'NEIROETHUSDT', 'RPLUSDT', 'POLUSDT', 'UXLINKUSDT', 'NEIROUSDT', '1MBABYDOGEUSDT', 'KDAUSDT', 'FIDAUSDT', 'FIOUSDT', 'CATIUSDT', 'GHSTUSDT', 'LOKAUSDT', 'HMSTRUSDT', 'REIUSDT', 'COSUSDT', 'EIGENUSDT', 'DIAUSDT', '1000CATUSDT', 'SCRUSDT', 'GOATUSDT', 'MOODENGUSDT', 'SAFEUSDT', 'SANTOSUSDT', 'TROYUSDT', 'PONKEUSDT', 'CETUSUSDT', 'COWUSDT', '1000000MOGUSDT', 'GRASSUSDT', 'DRIFTUSDT', 'SWELLUSDT', 'PNUTUSDT', 'ACTUSDT', 'HIPPOUSDT', '1000XUSDT', 'DEGENUSDT', 'BANUSDT', 'AKTUSDT', 'SLERFUSDT', 'SCRTUSDT', '1000WHYUSDT', '1000CHEEMSUSDT', 'THEUSDT', 'MORPHOUSDT', 'CHILLGUYUSDT', 'KAIAUSDT', 'AEROUSDT', 'ACXUSDT', 'ORCAUSDT', 'MOVEUSDT', 'RAYSOLUSDT', 'VIRTUALUSDT', 'SPXUSDT', 'KOMAUSDT', 'MEUSDT', 'ABCCCUSDT', 'AVAUSDT', 'DEGOUSDT', 'VELODROMEUSDT', 'MOCAUSDT', 'VANAUSDT', 'PENGUUSDT', 'LUMIAUSDT', 'USUALUSDT', 'KMNOUSDT', 'CGPTUSDT', 'AIXBTUSDT', 'FARTCOINUSDT', 'HIVEUSDT', 'DEXEUSDT', 'PHAUSDT', 'DFUSDT', 'GRIFFAINUSDT', 'AI16ZUSDT', 'ZEREBROUSDT', 'BIOUSDT', 'COOKIEUSDT', 'ALCHUSDT', 'SWARMSUSDT', 'SONICUSDT', 'DUSDT', 'PROMUSDT', 'SUSDT', 'ARCUSDT', 'AVAAIUSDT', 'TRUMPUSDT', 'MELANIAUSDT', 'VTHOUSDT', 'ANIMEUSDT', 'PIPPINUSDT', 'VINEUSDT', 'VVVUSDT', 'BERAUSDT', 'TSTUSDT', 'LAYERUSDT', 'HEIUSDT', 'B3USDT', 'IPUSDT', 'GPSUSDT', 'SHELLUSDT', 'KAITOUSDT', 'REDUSDT', 'VICUSDT', 'BMTUSDT', 'MUBARAKUSDT', 'BROCCOLI714USDT', 'BROCCOLIF3BUSDT', 'BANANAS31USDT', 'SIRENUSDT', 'BNXUSDT', 'BRUSDT', 'PLUMEUSDT', 'PAXGUSDT', 'WALUSDT', 'FUNUSDT', 'MLNUSDT', 'ATHUSDT', 'BABYUSDT', 'FORTHUSDT', 'PROMPTUSDT', 'XCNUSDT', 'PUMPUSDT', 'STOUSDT', 'FHEUSDT', 'INITUSDT', 'BANKUSDT', 'EPTUSDT', 'DEEPUSDT', 'HYPERUSDT', 'MEMEFIUSDT', 'FISUSDT', 'JSTUSDT', 'SIGNUSDT', 'PUNDIXUSDT', 'AIOTUSDT', 'DOLOUSDT', 'HAEDALUSDT', 'SXTUSDT', 'ALPINEUSDT', 'ASRUSDT', 'B2USDT', 'MILKUSDT', 'SYRUPUSDT', 'OBOLUSDT', 'OGUSDT', 'ZKJUSDT', 'SKYAIUSDT', 'NXPCUSDT', 'AGTUSDT', 'BUSDT', 'AUSDT', 'MERLUSDT', 'HYPEUSDT', 'BDXNUSDT', 'PUFFERUSDT', 'PORT3USDT', '1000000BOBUSDT', 'SKATEUSDT', 'TAIKOUSDT', 'SQDUSDT']
interval = '4h'


# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Глобальные переменные
conf: db.ConfigInfo
client: binance.Futures
config = configparser.ConfigParser()


# symbols = ['BTCUSDT', 'ETHUSDT']

interval_minutes = 4 * 60  # для 4h

# Временной диапазон
start_date = datetime(2025, 2, 22, tzinfo=pytz.UTC)
end_date = datetime(2025, 6, 15, tzinfo=pytz.UTC)


# async def main():
#     config.read('config.ini')
#     session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
#                                                                config['DB']['password'], config['DB']['db'])
#     conf = await db.load_config()
#     client = binance.Futures(
#         conf.api_key, conf.api_secret,
#         asynced=True,
#         testnet=config.getboolean('BOT', 'testnet')
#     )
#     debug = config.getboolean('BOT', 'debug')
#
#     all_signals = []  # Собираем все сигналы здесь
#
#     for symbol in symbols:
#         current_time = start_date
#
#         while current_time <= end_date:
#             try:
#                 end_ts = int(current_time.timestamp() * 1000)
#
#                 klines = await client.klines(
#                     symbol,
#                     interval=interval,
#                     endTime=end_ts,
#                     limit=500
#                 )
#
#                 if not klines or len(klines) < 100:
#                     logging.warning(f"Недостаточно данных для {symbol} на {current_time}")
#                     current_time += timedelta(minutes=interval_minutes)
#                     continue
#
#                 rating = get_technical_rating(klines)
#                 last_close = float(klines[-1][4])
#                 timestamp_ms = int(klines[-1][0])
#                 utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
#
#                 all_signals.append({
#                     'symbol': symbol,
#                     'interval': interval,
#                     'signal': rating,
#                     'entry_price': last_close,
#                     'utc_time': utc_time.strftime('%Y-%m-%d %H:%M:%S')
#                 })
#
#                 # print(f"{symbol} {interval} @ {utc_time} — {rating}")
#
#             except Exception as e:
#                 logging.error(f"Ошибка для {symbol} @ {current_time}: {e}")
#
#             current_time += timedelta(minutes=interval_minutes)
#
#     # Сохраняем все сигналы сразу после сбора
#     save_signals_to_csv('signals_25.csv', all_signals)
#
#
# from tqdm import tqdm  # Добавь этот импорт в начало файла
#
# async def main():
#     config.read('config.ini')
#     session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
#                                config['DB']['password'], config['DB']['db'])
#     conf = await db.load_config()
#     client = binance.Futures(
#         conf.api_key, conf.api_secret,
#         asynced=True,
#         testnet=config.getboolean('BOT', 'testnet')
#     )
#     debug = config.getboolean('BOT', 'debug')
#
#     all_signals = []
#
#     for symbol in symbols:
#         current_time = start_date
#         total_steps = int(((end_date - start_date).total_seconds()) // (interval_minutes * 60))
#
#         with tqdm(total=total_steps, desc=f"{symbol} {interval}") as pbar:
#             while current_time <= end_date:
#                 try:
#                     end_ts = int(current_time.timestamp() * 1000)
#
#                     klines = await client.klines(
#                         symbol,
#                         interval=interval,
#                         endTime=end_ts,
#                         limit=500
#                     )
#
#                     if not klines or len(klines) < 100:
#                         logging.warning(f"Недостаточно данных для {symbol} на {current_time}")
#                         current_time += timedelta(minutes=interval_minutes)
#                         pbar.update(1)
#                         continue
#
#                     rating = get_technical_rating(klines)
#                     last_close = float(klines[-1][4])
#                     timestamp_ms = int(klines[-1][0])
#                     utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
#
#                     if symbol == 'BTCUSDT' or rating in ['STRONG_SELL', 'STRONG_BUY']:
#                         all_signals.append({
#                             'symbol': symbol,
#                             'interval': interval,
#                             'signal': rating,
#                             'entry_price': last_close,
#                             'utc_time': utc_time.strftime('%Y-%m-%d %H:%M:%S')
#                         })
#
#                 except Exception as e:
#                     logging.error(f"Ошибка для {symbol} @ {current_time}: {e}")
#
#                 current_time += timedelta(minutes=interval_minutes)
#                 pbar.update(1)
#
#     save_signals_to_csv('signals_25.csv', all_signals)






# from tqdm.asyncio import tqdm_asyncio  # tqdm для async
# import asyncio
# from tqdm import tqdm
#
#
#
#
# async def process_symbol(symbol, conf, client):
#     current_time = start_date
#     total_steps = int(((end_date - start_date).total_seconds()) // (interval_minutes * 60))
#
#     signals = []
#
#     with tqdm(total=total_steps, desc=f"{symbol} {interval}") as pbar:
#         while current_time <= end_date:
#             try:
#                 end_ts = int(current_time.timestamp() * 1000)
#
#                 klines = await client.klines(
#                     symbol,
#                     interval=interval,
#                     endTime=end_ts,
#                     limit=500
#                 )
#
#                 if not klines or len(klines) < 100:
#                     logging.warning(f"Недостаточно данных для {symbol} на {current_time}")
#                     current_time += timedelta(minutes=interval_minutes)
#                     pbar.update(1)
#                     continue
#
#                 rating = get_technical_rating(klines)
#                 last_close = float(klines[-1][4])
#                 timestamp_ms = int(klines[-1][0])
#                 utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
#
#                 if symbol == 'BTCUSDT' or rating in ['STRONG_SELL', 'STRONG_BUY']:
#                     signals.append({
#                         'symbol': symbol,
#                         'interval': interval,
#                         'signal': rating,
#                         'entry_price': last_close,
#                         'utc_time': utc_time.strftime('%Y-%m-%d %H:%M:%S')
#                     })
#
#             except Exception as e:
#                 logging.error(f"Ошибка для {symbol} @ {current_time}: {e}")
#
#             current_time += timedelta(minutes=interval_minutes)
#             pbar.update(1)
#
#     return signals
#
#
#
#
#
# async def main():
#     config.read('config.ini')
#     session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
#                                config['DB']['password'], config['DB']['db'])
#     conf = await db.load_config()
#     client = binance.Futures(
#         conf.api_key, conf.api_secret,
#         asynced=True,
#         testnet=config.getboolean('BOT', 'testnet')
#     )
#
#     tasks = [process_symbol(symbol, conf, client) for symbol in symbols]
#     all_results = await asyncio.gather(*tasks)
#
#     # Flatten results
#     all_signals = [signal for symbol_signals in all_results for signal in symbol_signals]
#
#     save_signals_to_csv('signals_25_new.csv', all_signals)
#
#
#
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     asyncio.run(main())

import asyncio
import logging
from tqdm import tqdm
from datetime import datetime, timedelta, timezone
import configparser
import db
import binance

# Настройки лимитов
BATCH_SIZE = 20                # Кол-во символов в одном батче
BATCH_DELAY = 10              # Задержка между батчами (сек)

config = configparser.ConfigParser()


async def process_symbol(symbol, conf, client):
    current_time = start_date
    total_steps = int(((end_date - start_date).total_seconds()) // (interval_minutes * 60))
    signals = []

    with tqdm(total=total_steps, desc=f"{symbol} {interval}") as pbar:
        while current_time <= end_date:
            try:
                end_ts = int(current_time.timestamp() * 1000)

                klines = await client.klines(
                    symbol,
                    interval=interval,
                    endTime=end_ts,
                    limit=500
                )

                if not klines or len(klines) < 100:
                    logging.warning(f"Недостаточно данных для {symbol} на {current_time}")
                    current_time += timedelta(minutes=interval_minutes)
                    pbar.update(1)
                    await asyncio.sleep(1)
                    continue

                rating = get_technical_rating(klines)
                last_close = float(klines[-1][4])
                timestamp_ms = int(klines[-1][0])
                utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

                if symbol == 'BTCUSDT' or rating in ['STRONG_SELL', 'STRONG_BUY']:
                    signals.append({
                        'symbol': symbol,
                        'interval': interval,
                        'signal': rating,
                        'entry_price': last_close,
                        'utc_time': utc_time.strftime('%Y-%m-%d %H:%M:%S')
                    })

            except Exception as e:
                logging.error(f"Ошибка для {symbol} @ {current_time}: {e}")

            current_time += timedelta(minutes=interval_minutes)
            pbar.update(1)

    return signals


async def process_batch(batch_symbols, conf, client):
    tasks = [process_symbol(symbol, conf, client) for symbol in batch_symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Обработка исключений внутри gather
    all_signals = []
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Ошибка в таске: {result}")
        else:
            all_signals.extend(result)
    return all_signals


async def main():
    global start_date, end_date, interval, interval_minutes, symbols

    config.read('config.ini')
    session = await db.connect(config['DB']['host'], int(config['DB']['port']), config['DB']['user'],
                               config['DB']['password'], config['DB']['db'])
    conf = await db.load_config()
    client = binance.Futures(
        conf.api_key, conf.api_secret,
        asynced=True,
        testnet=config.getboolean('BOT', 'testnet')
    )


    all_signals = []

    # Разбиваем на батчи
    for i in range(0, len(symbols), BATCH_SIZE):
        batch_symbols = symbols[i:i+BATCH_SIZE]
        logging.info(f"Запуск батча: {batch_symbols}")

        batch_signals = await process_batch(batch_symbols, conf, client)
        all_signals.extend(batch_signals)

        # Задержка между батчами
        if i + BATCH_SIZE < len(symbols):
            await asyncio.sleep(BATCH_DELAY)

    # Сохраняем результат
    save_signals_to_csv('signals_29_new.csv', all_signals)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
