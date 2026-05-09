import logging
logging.basicConfig(level=logging.INFO)
import db
import traceback
from datetime import datetime, timezone, timedelta, time
from tradingview_ta import TA_Handler, Interval



async def load_binance_symbols(client):
    try:
        symbols_data = await client.load_symbols()
        all_symbols = {
            symbol: value for symbol, value in symbols_data.items()
            if symbol.endswith('USDT') and 'USDC' not in symbol and value.status == 'TRADING'
        }
        symbols = [s.symbol for s in all_symbols.values()]
        logging.info(f"Загружены символы: {symbols}")
        await db.update_binance_symbols_db(symbols)
        return all_symbols
    except Exception as e:
        logging.error(f"Ошибка при загрузке символов: {e}")




#
# import asyncio
# import logging
# import traceback
# from tradingview_ta import TA_Handler, Interval
#
# INTERVAL_MAPPING = {
#     "1m": Interval.INTERVAL_1_MINUTE,
#     "5m": Interval.INTERVAL_5_MINUTES,
#     "15m": Interval.INTERVAL_15_MINUTES,
#     "30m": Interval.INTERVAL_30_MINUTES,
#     "1h": Interval.INTERVAL_1_HOUR,
#     "4h": Interval.INTERVAL_4_HOURS,
#     "1d": Interval.INTERVAL_1_DAY,
# }
#
# # Ограничение на одновременные запросы
# semaphore = asyncio.Semaphore(5)  # не больше 5 параллельно
#
# async def get_tradingview_data(symbol, timeframe, retries=2):
#     interval = INTERVAL_MAPPING[timeframe]
#     for attempt in range(retries):
#         try:
#             async with semaphore:
#                 await asyncio.sleep(1)  # минимальная задержка между запросами
#                 handler = TA_Handler(
#                     symbol=symbol,
#                     exchange='Binance',
#                     screener="crypto",
#                     interval=interval
#                 )
#                 analysis = handler.get_analysis().summary
#                 analysis['SYMBOL'] = symbol
#                 return analysis
#         except Exception as e:
#             logging.warning(f"TV ошибка {symbol} ({timeframe}), попытка {attempt + 1}: {e}\n{traceback.format_exc()}")
#     return None



def get_tradingview_data(symbol, timeframe, retries=1):

    INTERVAL_MAPPING = {
        "1m": Interval.INTERVAL_1_MINUTE,
        "5m": Interval.INTERVAL_5_MINUTES,
        "15m": Interval.INTERVAL_15_MINUTES,
        "30m": Interval.INTERVAL_30_MINUTES,
        "1h": Interval.INTERVAL_1_HOUR,
        "4h": Interval.INTERVAL_4_HOURS,
        "1d": Interval.INTERVAL_1_DAY,
    }

    interval = INTERVAL_MAPPING[timeframe]
    for attempt in range(retries):
        try:
            handler = TA_Handler(
                symbol=symbol,
                exchange='Binance',
                screener="crypto",
                interval=interval
            )
            analysis = handler.get_analysis().summary
            analysis['SYMBOL'] = symbol
            return analysis
        except Exception as e:
            logging.warning(f"TV ошибка {symbol} ({timeframe}), попытка {attempt + 1}: {e}")
            # if attempt < retries - 1:
            #     time.sleep(0.5)
        except Exception as e:
            logging.warning(f"TV ошибка {symbol} ({timeframe}), попытка {attempt + 1}: {e}\n{traceback.format_exc()}")
    return None



async def get_all_prices(client) -> dict[str, float]:
    try:
        prices_raw = await client.mark_price()
        prices = {item['symbol']: float(item['markPrice']) for item in prices_raw}
        # print(f"PRICES {prices} TIME: {datetime.now(timezone.utc)}")
        return prices
    except Exception as e:
        logging.error(f"Ошибка при получении цен: {e}")
        return


async def sync_positions_with_exchange(client, positions: dict):
    position_info = await client.get_position_risk()
    # print("sync_positions_with_exchange")
    for pos in position_info:
        symbol = pos['symbol']
        amt = float(pos['positionAmt'])

        if amt != 0.0:
            if symbol not in positions or not positions[symbol]:
                # print(f"[SYNC] Найдена активная позиция на бирже: {symbol}")
                positions[symbol] = True
        else:
            if symbol not in positions:
                positions[symbol] = False
