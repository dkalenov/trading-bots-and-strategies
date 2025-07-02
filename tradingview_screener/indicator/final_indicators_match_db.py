import pandas as pd
from tradingview_ta import TA_Handler, Interval
import talib
import time
import binance
import numpy as np
import pprint as pp
from sqlalchemy import create_engine, Column, String, Float, MetaData, Table, Integer, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta
from datetime import datetime, timezone


Base = declarative_base()



target_indicators = [
    'EMA10', 'EMA20', 'EMA30', 'EMA50', 'EMA100', 'EMA200',
    'SMA10', 'SMA20', 'SMA30', 'SMA50', 'SMA100', 'SMA200',
    'VWMA', 'HullMA', 'Ichimoku',
    'RSI', 'STOCH.K', 'Stoch.RSI', 'CCI', 'ADX',
    'AO', 'Mom', 'MACD', 'W%R', 'BBP', 'UO'
]



indicator_mapping = {
    'RSI': ('RSI', 'RSI'),
    'MACD': ('MACD.macd', 'MACD'),
    'EMA10': ('EMA10', 'EMA10'),
    'EMA20': ('EMA20', 'EMA20'),
    'EMA30': ('EMA30', 'EMA30'),
    'EMA50': ('EMA50', 'EMA50'),
    'EMA100': ('EMA100', 'EMA100'),
    'EMA200': ('EMA200', 'EMA200'),
    'SMA10': ('SMA10', 'SMA10'),
    'SMA20': ('SMA20', 'SMA20'),
    'SMA30': ('SMA30', 'SMA30'),
    'SMA50': ('SMA50', 'SMA50'),
    'SMA100': ('SMA100', 'SMA100'),
    'SMA200': ('SMA200', 'SMA200'),
    'VWMA': ('VWMA', 'VWMA'),
    'HullMA': ('HullMA9', 'HullMA'),
    'Ichimoku': ('Ichimoku.BLine', 'Ichimoku'),
    'STOCH.K': ('Stoch.K', 'STOCH.K'),
    'Stoch.RSI': ('Stoch.RSI.K', 'Stoch.RSI'),
    'CCI': ('CCI20', 'CCI'),
    'ADX': ('ADX', 'ADX'),
    'AO': ('AO', 'AO'),
    'Mom': ('Mom', 'Mom'),
    'W%R': ('W.R', 'W%R'),
    'BBP': ('BBPower', 'BBP'),
    'UO': ('UO', 'UO'),
}


class IndicatorData(Base):
    __tablename__ = 'indicator_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    interval = Column(String)
    tv_signal = Column(String)
    entry_price = Column(Float)
    utc_time = Column(DateTime(timezone=True))
    atr = Column(Float)

    for ind in target_indicators:
        vars()[f"{ind}_tv_value"] = Column(Float)
        vars()[f"{ind}_tv_signal"] = Column(String)
        vars()[f"{ind}_manual_value"] = Column(Float)


# === DB Подключение ===

DATABASE_URL = "URL OF YOUR DATABASE"
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)



api_secret = 'YOYR API SECRET'
api_key = 'YOUR API'

client = binance.Futures(api_key, api_secret, asynced=False, testnet=False)


symbols = ['BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'LTCUSDT', 'TRXUSDT', 'ETCUSDT', 'LINKUSDT', 'XLMUSDT', 'ADAUSDT', 'DASHUSDT', 'ZECUSDT', 'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT', 'IOTAUSDT', 'BATUSDT', 'VETUSDT', 'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT', 'KNCUSDT', 'ZRXUSDT', 'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'KAVAUSDT', 'BANDUSDT', 'RLCUSDT', 'MKRUSDT', 'SNXUSDT', 'DOTUSDT', 'DEFIUSDT', 'YFIUSDT', 'BALUSDT', 'CRVUSDT', 'TRBUSDT', 'RUNEUSDT', 'SUSHIUSDT', 'EGLDUSDT', 'SOLUSDT', 'ICXUSDT', 'STORJUSDT', 'UNIUSDT', 'AVAXUSDT', 'ENJUSDT', 'FLMUSDT', 'KSMUSDT', 'NEARUSDT', 'AAVEUSDT', 'FILUSDT', 'RSRUSDT', 'LRCUSDT', 'BELUSDT', 'AXSUSDT', 'ALPHAUSDT', 'ZENUSDT', 'SKLUSDT', 'GRTUSDT', 'BNTUSDT', '1INCHUSDT', 'CHZUSDT', 'SANDUSDT', 'ANKRUSDT', 'LITUSDT', 'RVNUSDT', 'SFPUSDT', 'XEMUSDT', 'COTIUSDT', 'CHRUSDT', 'MANAUSDT', 'ALICEUSDT', 'HBARUSDT', 'ONEUSDT', 'LINAUSDT', 'STMXUSDT', 'DENTUSDT', 'CELRUSDT', 'HOTUSDT', 'MTLUSDT', 'OGNUSDT', 'NKNUSDT', '1000SHIBUSDT', 'BAKEUSDT', 'GTCUSDT', 'BTCDOMUSDT', 'MASKUSDT', 'IOTXUSDT', 'C98USDT', 'ATAUSDT', 'DYDXUSDT', '1000XECUSDT', 'GALAUSDT', 'CELOUSDT', 'ARUSDT', 'ARPAUSDT', 'CTSIUSDT', 'LPTUSDT', 'ENSUSDT', 'PEOPLEUSDT', 'ROSEUSDT', 'DUSKUSDT', 'FLOWUSDT', 'IMXUSDT', 'API3USDT', 'GMTUSDT', 'APEUSDT', 'WOOUSDT', 'JASMYUSDT', 'OPUSDT', 'XMRUSDT', 'INJUSDT', 'STGUSDT', 'SPELLUSDT', '1000LUNCUSDT', 'LUNA2USDT', 'LDOUSDT', 'APTUSDT', 'QNTUSDT', 'FXSUSDT', 'HOOKUSDT', 'MAGICUSDT', 'TUSDT', 'HIGHUSDT', 'MINAUSDT', 'ASTRUSDT', 'PHBUSDT', 'FETUSDT', 'GMXUSDT', 'CFXUSDT', 'STXUSDT', 'ACHUSDT', 'SSVUSDT', 'CKBUSDT', 'PERPUSDT', 'LQTYUSDT', 'ARBUSDT', 'IDUSDT', 'JOEUSDT', 'LEVERUSDT', 'TRUUSDT', 'RDNTUSDT', 'HFTUSDT', 'XVSUSDT', 'BLURUSDT', 'EDUUSDT', '1000PEPEUSDT', '1000FLOKIUSDT', 'UMAUSDT', 'COMBOUSDT', 'SUIUSDT', 'NMRUSDT', 'MAVUSDT', 'XVGUSDT', 'WLDUSDT', 'PENDLEUSDT', 'ARKMUSDT', 'AGLDUSDT', 'YGGUSDT', 'DODOXUSDT', 'OXTUSDT', 'SEIUSDT', 'CYBERUSDT', 'HIFIUSDT', 'ARKUSDT', 'BICOUSDT', 'LOOMUSDT', 'BIGTIMEUSDT', 'BONDUSDT', 'ORBSUSDT', 'WAXPUSDT', 'BSVUSDT', 'RIFUSDT', 'POLYXUSDT', 'GASUSDT', 'POWRUSDT', 'TIAUSDT', 'CAKEUSDT', 'MEMEUSDT', 'TOKENUSDT', 'ORDIUSDT', 'STEEMUSDT', 'ILVUSDT', 'NTRNUSDT', 'KASUSDT', 'BEAMXUSDT', '1000BONKUSDT', 'PYTHUSDT', 'SUPERUSDT', 'USTCUSDT', 'ONGUSDT', 'ETHWUSDT', 'JTOUSDT', '1000SATSUSDT', 'AUCTIONUSDT', '1000RATSUSDT', 'ACEUSDT', 'MOVRUSDT', 'TWTUSDT', 'NFPUSDT', 'AIUSDT', 'XAIUSDT', 'WIFUSDT', 'MANTAUSDT', 'ONDOUSDT', 'LSKUSDT', 'ALTUSDT', 'JUPUSDT', 'ZETAUSDT', 'RONINUSDT', 'DYMUSDT', 'OMUSDT', 'PIXELUSDT', 'STRKUSDT', 'GLMUSDT', 'PORTALUSDT', 'TONUSDT', 'AXLUSDT', 'MYROUSDT', 'METISUSDT', 'AEVOUSDT', 'VANRYUSDT', 'BOMEUSDT', 'ETHFIUSDT', 'ENAUSDT', 'WUSDT', 'TNSRUSDT', 'SAGAUSDT', 'TAOUSDT', 'OMNIUSDT', 'REZUSDT', 'BBUSDT', 'NOTUSDT', 'TURBOUSDT', 'IOUSDT', 'ZKUSDT', 'MEWUSDT', 'LISTAUSDT', 'ZROUSDT', 'RENDERUSDT', 'BANANAUSDT', 'RAREUSDT', 'GUSDT', 'SYNUSDT', 'SYSUSDT', 'VOXELUSDT', 'BRETTUSDT', 'ALPACAUSDT', 'POPCATUSDT', 'SUNUSDT', 'VIDTUSDT', 'NULSUSDT', 'DOGSUSDT', 'MBOXUSDT', 'CHESSUSDT', 'FLUXUSDT', 'BSWUSDT', 'QUICKUSDT', 'NEIROETHUSDT', 'RPLUSDT', 'POLUSDT', 'UXLINKUSDT', 'NEIROUSDT', '1MBABYDOGEUSDT', 'KDAUSDT', 'FIDAUSDT', 'FIOUSDT', 'CATIUSDT', 'GHSTUSDT', 'LOKAUSDT', 'HMSTRUSDT', 'REIUSDT', 'COSUSDT', 'EIGENUSDT', 'DIAUSDT', '1000CATUSDT', 'SCRUSDT', 'GOATUSDT', 'MOODENGUSDT', 'SAFEUSDT', 'SANTOSUSDT', 'TROYUSDT', 'PONKEUSDT', 'CETUSUSDT', 'COWUSDT', '1000000MOGUSDT', 'GRASSUSDT', 'DRIFTUSDT', 'SWELLUSDT', 'PNUTUSDT', 'ACTUSDT', 'HIPPOUSDT', '1000XUSDT', 'DEGENUSDT', 'BANUSDT', 'AKTUSDT', 'SLERFUSDT', 'SCRTUSDT', '1000WHYUSDT', '1000CHEEMSUSDT', 'THEUSDT', 'MORPHOUSDT', 'CHILLGUYUSDT', 'KAIAUSDT', 'AEROUSDT', 'ACXUSDT', 'ORCAUSDT', 'MOVEUSDT', 'RAYSOLUSDT', 'VIRTUALUSDT', 'SPXUSDT', 'KOMAUSDT', 'MEUSDT', 'ABCCCUSDT', 'AVAUSDT', 'DEGOUSDT', 'VELODROMEUSDT', 'MOCAUSDT', 'VANAUSDT', 'PENGUUSDT', 'LUMIAUSDT', 'USUALUSDT', 'KMNOUSDT', 'CGPTUSDT', 'AIXBTUSDT', 'FARTCOINUSDT', 'HIVEUSDT', 'DEXEUSDT', 'PHAUSDT', 'DFUSDT', 'GRIFFAINUSDT', 'AI16ZUSDT', 'ZEREBROUSDT', 'BIOUSDT', 'COOKIEUSDT', 'ALCHUSDT', 'SWARMSUSDT', 'SONICUSDT', 'DUSDT', 'PROMUSDT', 'SUSDT', 'ARCUSDT', 'AVAAIUSDT', 'TRUMPUSDT', 'MELANIAUSDT', 'VTHOUSDT', 'ANIMEUSDT', 'PIPPINUSDT', 'VINEUSDT', 'VVVUSDT', 'BERAUSDT', 'TSTUSDT', 'LAYERUSDT', 'HEIUSDT', 'B3USDT', 'IPUSDT', 'GPSUSDT', 'SHELLUSDT', 'KAITOUSDT', 'REDUSDT', 'VICUSDT', 'BMTUSDT', 'MUBARAKUSDT', 'BROCCOLI714USDT', 'BROCCOLIF3BUSDT', 'BANANAS31USDT', 'SIRENUSDT', 'BNXUSDT', 'BRUSDT', 'PLUMEUSDT', 'PAXGUSDT', 'WALUSDT', 'FUNUSDT', 'MLNUSDT', 'ATHUSDT', 'BABYUSDT', 'FORTHUSDT', 'PROMPTUSDT', 'XCNUSDT', 'PUMPUSDT', 'STOUSDT', 'FHEUSDT', 'INITUSDT', 'BANKUSDT', 'EPTUSDT', 'DEEPUSDT', 'HYPERUSDT', 'MEMEFIUSDT', 'FISUSDT', 'JSTUSDT', 'SIGNUSDT', 'PUNDIXUSDT', 'AIOTUSDT', 'DOLOUSDT', 'HAEDALUSDT', 'SXTUSDT', 'ALPINEUSDT', 'ASRUSDT', 'B2USDT', 'MILKUSDT', 'SYRUPUSDT', 'OBOLUSDT', 'OGUSDT', 'ZKJUSDT', 'SKYAIUSDT', 'NXPCUSDT', 'AGTUSDT', 'BUSDT', 'AUSDT', 'MERLUSDT', 'HYPEUSDT', 'BDXNUSDT', 'PUFFERUSDT', 'PORT3USDT', '1000000BOBUSDT', 'SKATEUSDT', 'TAIKOUSDT', 'SQDUSDT']
# symbols = ['KASUSDT', 'BEAMXUSDT', '1000BONKUSDT', 'PYTHUSDT', 'SUPERUSDT', 'USTCUSDT', 'ONGUSDT', 'ETHWUSDT', 'JTOUSDT', '1000SATSUSDT', 'AUCTIONUSDT', '1000RATSUSDT', 'ACEUSDT', 'MOVRUSDT', 'TWTUSDT', 'NFPUSDT', 'AIUSDT', 'XAIUSDT', 'WIFUSDT', 'MANTAUSDT', 'ONDOUSDT', 'LSKUSDT', 'ALTUSDT', 'JUPUSDT', 'ZETAUSDT', 'RONINUSDT', 'DYMUSDT', 'OMUSDT', 'PIXELUSDT', 'STRKUSDT', 'GLMUSDT', 'PORTALUSDT', 'TONUSDT', 'AXLUSDT', 'MYROUSDT', 'METISUSDT', 'AEVOUSDT', 'VANRYUSDT', 'BOMEUSDT', 'ETHFIUSDT', 'ENAUSDT', 'WUSDT', 'TNSRUSDT', 'SAGAUSDT', 'TAOUSDT', 'OMNIUSDT', 'REZUSDT', 'BBUSDT', 'NOTUSDT', 'TURBOUSDT', 'IOUSDT', 'ZKUSDT', 'MEWUSDT', 'LISTAUSDT', 'ZROUSDT', 'RENDERUSDT', 'BANANAUSDT', 'RAREUSDT', 'GUSDT', 'SYNUSDT', 'SYSUSDT', 'VOXELUSDT', 'BRETTUSDT', 'ALPACAUSDT', 'POPCATUSDT', 'SUNUSDT', 'VIDTUSDT', 'NULSUSDT', 'DOGSUSDT', 'MBOXUSDT', 'CHESSUSDT', 'FLUXUSDT', 'BSWUSDT', 'QUICKUSDT', 'NEIROETHUSDT', 'RPLUSDT', 'POLUSDT', 'UXLINKUSDT', 'NEIROUSDT', '1MBABYDOGEUSDT', 'KDAUSDT', 'FIDAUSDT', 'FIOUSDT', 'CATIUSDT', 'GHSTUSDT', 'LOKAUSDT', 'HMSTRUSDT', 'REIUSDT', 'COSUSDT', 'EIGENUSDT', 'DIAUSDT', '1000CATUSDT', 'SCRUSDT', 'GOATUSDT', 'MOODENGUSDT', 'SAFEUSDT', 'SANTOSUSDT', 'TROYUSDT', 'PONKEUSDT', 'CETUSUSDT', 'COWUSDT', '1000000MOGUSDT', 'GRASSUSDT', 'DRIFTUSDT', 'SWELLUSDT', 'PNUTUSDT', 'ACTUSDT', 'HIPPOUSDT', '1000XUSDT', 'DEGENUSDT', 'BANUSDT', 'AKTUSDT', 'SLERFUSDT', 'SCRTUSDT', '1000WHYUSDT', '1000CHEEMSUSDT', 'THEUSDT', 'MORPHOUSDT', 'CHILLGUYUSDT', 'KAIAUSDT', 'AEROUSDT', 'ACXUSDT', 'ORCAUSDT', 'MOVEUSDT', 'RAYSOLUSDT', 'VIRTUALUSDT', 'SPXUSDT', 'KOMAUSDT', 'MEUSDT', 'ABCCCUSDT', 'AVAUSDT', 'DEGOUSDT', 'VELODROMEUSDT', 'MOCAUSDT', 'VANAUSDT', 'PENGUUSDT', 'LUMIAUSDT', 'USUALUSDT', 'KMNOUSDT', 'CGPTUSDT', 'AIXBTUSDT', 'FARTCOINUSDT', 'HIVEUSDT', 'DEXEUSDT', 'PHAUSDT', 'DFUSDT', 'GRIFFAINUSDT', 'AI16ZUSDT', 'ZEREBROUSDT', 'BIOUSDT', 'COOKIEUSDT', 'ALCHUSDT', 'SWARMSUSDT', 'SONICUSDT', 'DUSDT', 'PROMUSDT', 'SUSDT', 'ARCUSDT', 'AVAAIUSDT', 'TRUMPUSDT', 'MELANIAUSDT', 'VTHOUSDT', 'ANIMEUSDT', 'PIPPINUSDT', 'VINEUSDT', 'VVVUSDT', 'BERAUSDT', 'TSTUSDT', 'LAYERUSDT', 'HEIUSDT', 'B3USDT', 'IPUSDT', 'GPSUSDT', 'SHELLUSDT', 'KAITOUSDT', 'REDUSDT', 'VICUSDT', 'BMTUSDT', 'MUBARAKUSDT', 'BROCCOLI714USDT', 'BROCCOLIF3BUSDT', 'BANANAS31USDT', 'SIRENUSDT', 'BNXUSDT', 'BRUSDT', 'PLUMEUSDT', 'PAXGUSDT', 'WALUSDT', 'FUNUSDT', 'MLNUSDT', 'ATHUSDT', 'BABYUSDT', 'FORTHUSDT', 'PROMPTUSDT', 'XCNUSDT', 'PUMPUSDT', 'STOUSDT', 'FHEUSDT', 'INITUSDT', 'BANKUSDT', 'EPTUSDT', 'DEEPUSDT', 'HYPERUSDT', 'MEMEFIUSDT', 'FISUSDT', 'JSTUSDT', 'SIGNUSDT', 'PUNDIXUSDT', 'AIOTUSDT', 'DOLOUSDT', 'HAEDALUSDT', 'SXTUSDT', 'ALPINEUSDT', 'ASRUSDT', 'B2USDT', 'MILKUSDT', 'SYRUPUSDT', 'OBOLUSDT', 'OGUSDT', 'ZKJUSDT', 'SKYAIUSDT', 'NXPCUSDT', 'AGTUSDT', 'BUSDT', 'AUSDT', 'MERLUSDT', 'HYPEUSDT', 'BDXNUSDT', 'PUFFERUSDT', 'PORT3USDT', '1000000BOBUSDT', 'SKATEUSDT', 'TAIKOUSDT', 'SQDUSDT']



intervals = {
    '15m': Interval.INTERVAL_15_MINUTES,
    '1h': Interval.INTERVAL_1_HOUR,
    '4h': Interval.INTERVAL_4_HOURS
}
binance_interval_map = {
    '15m': '15m',
    '1h': '1h',
    '4h': '4h'
}

# 🧠 Функция для интерпретации сигнала
def classify_signal(value, low=30, high=70, reverse=False):
    if value is None:
        return 'N/A'
    if reverse:
        return 'SELL' if value < low else 'BUY' if value > high else 'NEUTRAL'
    return 'BUY' if value < low else 'SELL' if value > high else 'NEUTRAL'

def safe_last(series):
    if isinstance(series, np.ndarray):
        series = pd.Series(series)
    try:
        return series.dropna().iloc[-1]
    except Exception:
        return None

def get_klines(symbol, interval, limit=500):
    try:
        return client.klines(symbol=symbol, interval=interval, limit=limit)
    except Exception as e:
        print(f"❌ Binance error {symbol}: {e}")
        return None

def get_talib_indicators(klines):
    df = pd.DataFrame(klines, columns=['timestamp','open','high','low','close','volume'] + ['_']*6).astype(float)
    close = df['close']
    high = df['high']
    low = df['low']
    volume = df['volume']
    current_price = close.iloc[-1]

    indicators = {}

    def safe_last(series):
        if isinstance(series, np.ndarray):
            series = pd.Series(series)
        try:
            return series.dropna().iloc[-1]
        except Exception:
            return None

    def classify_signal(value, low=30, high=70, reverse=False):
        if value is None:
            return 'N/A'
        if reverse:
            return 'SELL' if value < low else 'BUY' if value > high else 'NEUTRAL'
        return 'BUY' if value < low else 'SELL' if value > high else 'NEUTRAL'

    # RSI
    rsi = talib.RSI(close)
    indicators['RSI'] = (safe_last(rsi), classify_signal(safe_last(rsi)))

    # MACD
    macd, signal, _ = talib.MACD(close)
    diff = macd - signal
    indicators['MACD'] = (safe_last(macd), classify_signal(safe_last(diff), 0, 0))

    # EMA / SMA
    for p in [10, 20, 30, 50, 100, 200]:
        ema = talib.EMA(close, timeperiod=p)
        sma = talib.SMA(close, timeperiod=p)
        indicators[f'EMA{p}'] = (safe_last(ema), classify_signal(current_price - safe_last(ema), 0, 0))
        indicators[f'SMA{p}'] = (safe_last(sma), classify_signal(current_price - safe_last(sma), 0, 0))

    # ADX
    adx = talib.ADX(high, low, close)
    indicators['ADX'] = (safe_last(adx), classify_signal(safe_last(adx), 20, 40))

    # STOCH
    slowk, slowd = talib.STOCH(high, low, close, fastk_period=14, slowk_period=3, slowd_period=3)
    indicators['STOCH.K'] = (safe_last(slowk), classify_signal(safe_last(slowk)))

    # StochRSI
    rsi_ = talib.RSI(close, timeperiod=14)
    min_rsi = rsi_.rolling(14).min()
    max_rsi = rsi_.rolling(14).max()
    stochrsi = 100 * (rsi_ - min_rsi) / (max_rsi - min_rsi)
    indicators['Stoch.RSI'] = (safe_last(stochrsi), classify_signal(safe_last(stochrsi), 20, 80))

    # CCI
    cci = talib.CCI(high, low, close, timeperiod=20)
    indicators['CCI'] = (safe_last(cci), classify_signal(safe_last(cci), -100, 100, reverse=True))

    # Momentum
    mom = talib.MOM(close, timeperiod=10)
    indicators['Mom'] = (safe_last(mom), classify_signal(safe_last(mom), 0, 0))

    # ATR
    atr = talib.ATR(high, low, close, timeperiod=14)
    indicators['ATR'] = (safe_last(atr), 'NEUTRAL')

    # VWMA
    vwma = (close * volume).rolling(20).sum() / volume.rolling(20).sum()
    indicators['VWMA'] = (safe_last(vwma), classify_signal(current_price - safe_last(vwma), 0, 0))

    # HullMA
    def WMA(series, period):
        weights = np.arange(1, period + 1)
        return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)

    hull_raw = 2 * WMA(close, 9 // 2) - WMA(close, 9)
    hull = WMA(hull_raw, int(np.sqrt(9)))
    indicators['HullMA'] = (safe_last(hull), classify_signal(current_price - safe_last(hull), 0, 0))

    # Ichimoku base line
    base_line = (high.rolling(26).max() + low.rolling(26).min()) / 2
    indicators['Ichimoku'] = (safe_last(base_line), classify_signal(current_price - safe_last(base_line), 0, 0))

    # AO (Awesome Oscillator)
    median_price = (high + low) / 2
    ao = talib.SMA(median_price, timeperiod=5) - talib.SMA(median_price, timeperiod=34)
    indicators['AO'] = (safe_last(ao), classify_signal(safe_last(ao), 0, 0))

    # W%R
    wpr = talib.WILLR(high, low, close, timeperiod=14)
    indicators['W%R'] = (safe_last(wpr), classify_signal(safe_last(wpr), -80, -20, reverse=True))


    # BBP
    # upper, middle, lower = talib.BBANDS(close, timeperiod=20)
    # bbp = (close - lower) / (upper - lower)
    # indicators['BBP'] = (safe_last(bbp), classify_signal(safe_last(bbp), 0.2, 0.8))

    # TV BBPower (TV BBPower = Close - Middle Band)
    upper, middle, lower = talib.BBANDS(close, timeperiod=20)
    bbpower = close - middle
    indicators['BBP'] = (safe_last(bbpower), classify_signal(safe_last(bbpower), 0, 0))


    # Ultimate Oscillator
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    bp = close - pd.concat([low, close.shift()], axis=1).min(axis=1)
    avg7 = bp.rolling(7).sum() / tr.rolling(7).sum()
    avg14 = bp.rolling(14).sum() / tr.rolling(14).sum()
    avg28 = bp.rolling(28).sum() / tr.rolling(28).sum()
    uo = 100 * (4 * avg7 + 2 * avg14 + avg28) / 7
    indicators['UO'] = (safe_last(uo), classify_signal(safe_last(uo), 30, 70))

    last_timestamp = df['timestamp'].iloc[-1]
    utc_time = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc)

    atr = indicators.get("ATR", (None,))[0]

    return indicators, current_price, utc_time, atr


for symbol in symbols:
    results = []  # ← Сбрасываем при каждой новой монете

    for tf_key, tf_val in intervals.items():
        try:
            print(f"⏳ {symbol} {tf_key}...")

            handler = TA_Handler(
                symbol=symbol,
                screener="crypto",
                exchange="Binance",
                interval=tf_val
            )
            analysis = handler.get_analysis()
            tv_signal = analysis.summary.get('RECOMMENDATION', None)

            tv_values = analysis.indicators

            tv_signals = {
                **analysis.moving_averages.get('COMPUTE', {}),
                **analysis.oscillators.get('COMPUTE', {})
            }

            klines = get_klines(symbol, binance_interval_map[tf_key])
            if not klines or len(klines) < 50:
                raise Exception("Not enough candles")

            talib_data, entry_price, utc_time, atr = get_talib_indicators(klines)

            row = {
                'symbol': symbol,
                'interval': tf_key,
                'tv_signal': tv_signal,
                'entry_price': entry_price,
                'utc_time': utc_time,
                'atr': atr
            }

            for ind in target_indicators:
                val_name, sig_name = indicator_mapping.get(ind, (ind, ind))

                # TradingView values
                row[f"{ind}_tv_value"] = tv_values.get(val_name, None)
                row[f"{ind}_tv_signal"] = tv_signals.get(sig_name, 'N/A')

                # TA-Lib values
                manual_val, manual_sig = talib_data.get(ind, (None, 'N/A'))
                row[f"{ind}_manual_value"] = manual_val

            results.append(row)
            print(f"✅ {symbol} {tf_key} OK")

        except Exception as e:
            print(f"❌ Error {symbol} {tf_key}: {e}")
        time.sleep(30)

    # === Сохраняем данные в БД после обработки всех таймфреймов для одной монеты ===
    session = SessionLocal()
    for row in results:
        db_row = IndicatorData(
            symbol=row['symbol'],
            interval=row['interval'],
            tv_signal=row['tv_signal'],
        )


        def to_float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return None


        db_row.entry_price = to_float(row.get('entry_price'))
        db_row.atr = to_float(row.get('atr'))
        db_row.utc_time = row.get('utc_time')  # Already datetime with tz

        for ind in target_indicators:
            setattr(db_row, f"{ind}_tv_value", to_float(row.get(f"{ind}_tv_value")))
            setattr(db_row, f"{ind}_tv_signal", row.get(f"{ind}_tv_signal"))
            setattr(db_row, f"{ind}_manual_value", to_float(row.get(f"{ind}_manual_value")))

        session.merge(db_row)

    session.commit()
    session.close()




