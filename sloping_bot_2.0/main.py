import asyncio
import configparser
import traceback
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import update as sa_update

import binance
import db
import sloping
import tg
import utils


BASE_DIR = Path(__file__).resolve().parent

config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

conf: db.ConfigInfo
client: binance.Futures
all_symbols: dict[str, binance.SymbolFutures] = {}
indicators: dict[str, sloping.Sloping] = {}
positions: dict[str, bool] = {}
websockets_list: list[binance.futures.WebsocketAsync] = []
userdata_ws: binance.futures.WebsocketAsync | None = None
price_ws: binance.futures.WebsocketAsync | None = None
price_streams: set[str] = set()
session: db.async_sessionmaker
_take1_retry_after: dict[str, float] = {}
TAKE1_RETRY_COOLDOWN_SEC = 1.0


@dataclass
class TP1Watch:
    trade_id: int
    side: bool
    take1_price: float


tp1_watch: dict[str, TP1Watch] = {}
_take1_processing: set[str] = set()  # символы, для которых уже запущен partial close


async def main():
    global session
    global conf
    global client
    global all_symbols

    session = await db.connect(
        config["DB"]["host"],
        int(config["DB"]["port"]),
        config["DB"]["user"],
        config["DB"]["password"],
        config["DB"]["db"],
    )
    conf = await db.load_config()
    client = binance.Futures(
        conf.api_key,
        conf.api_secret,
        asynced=True,
        testnet=config.getboolean("BOT", "testnet"),
    )

    all_symbols = await client.load_symbols()
    asyncio.create_task(load_symbols())
    asyncio.create_task(connect_ws())
    await tg.run(session, client, connect_ws, disconnect_ws, subscribe_ws, unsubscribe_ws, remove_indicator)


async def load_symbols():
    global all_symbols

    while True:
        try:
            all_symbols = await client.load_symbols()
        except asyncio.CancelledError:
            try:
                await tg.dp.stop_polling()
            except Exception:
                pass
            break
        except Exception:
            pass
        await asyncio.sleep(3600)


async def sync_positions():
    global positions

    positions = {}
    if not conf.trade_mode or not conf.api_key or not conf.api_secret:
        return

    try:
        for pos in await client.get_position_risk():
            if float(pos["positionAmt"]) != 0:
                positions[pos["symbol"]] = True
    except Exception:
        print(f"Ошибка синхронизации открытых позиций\n{traceback.format_exc()}")


async def connect_ws():
    global websockets_list
    global userdata_ws
    global price_ws
    global conf

    if websockets_list or userdata_ws is not None:
        await disconnect_ws()

    conf = await db.load_config()
    if not conf.trade_mode:
        return

    if not conf.api_key or not conf.api_secret:
        await db.config_update(trade_mode="0")
        return

    await sync_positions()
    print("Подключаемся к вебсокетам")

    streams = []
    symbols = await db.get_all_symbols()
    for symbol in symbols:
        if symbol.status:
            streams.append(f"{symbol.symbol.lower()}@kline_{symbol.interval}")

    chunk_size = 100
    streams_list = [streams[i:i + chunk_size] for i in range(0, len(streams), chunk_size)]
    for stream_list in streams_list:
        websockets_list.append(await client.websocket(stream_list, on_message=ws_msg, on_error=ws_error))

    userdata_ws = await client.websocket_userdata(on_message=ws_user_msg, on_error=ws_error)
    await sync_tp1_price_streams()


async def disconnect_ws():
    global websockets_list
    global userdata_ws
    global price_ws
    global price_streams
    global indicators
    global positions
    global tp1_watch
    global _take1_retry_after

    print("Отключаемся от вебсокетов")
    for ws in websockets_list:
        try:
            await ws.close()
        except Exception:
            pass

    indicators = {}
    websockets_list = []

    try:
        if userdata_ws is not None:
            await userdata_ws.close()
    except Exception:
        pass

    try:
        if price_ws is not None:
            await price_ws.close()
    except Exception:
        pass

    userdata_ws = None
    price_ws = None
    price_streams = set()
    positions = {}
    tp1_watch = {}
    _take1_retry_after = {}


async def subscribe_ws(symbol, interval):
    global websockets_list

    stream = f"{symbol.lower()}@kline_{interval}"
    for ws in websockets_list:
        if len(ws.stream) < ws.streams_limit:
            await ws.subscribe([stream])
            break
    else:
        websockets_list.append(await client.websocket([stream], on_message=ws_msg, on_error=ws_error))


async def unsubscribe_ws(symbol):
    global websockets_list

    for ws in websockets_list:
        for st in list(ws.stream):
            if st.split("@")[0] == symbol.lower():
                await ws.unsubscribe(st)


def get_price_stream(symbol: str) -> str:
    return f"{symbol.lower()}@bookTicker"


async def subscribe_price_ws(symbol: str):
    global price_ws
    global price_streams

    stream = get_price_stream(symbol)
    if stream in price_streams:
        return

    if price_ws is None:
        price_ws = await client.websocket([stream], on_message=ws_price_msg, on_error=ws_error)
    else:
        await price_ws.subscribe([stream])

    price_streams.add(stream)


async def unsubscribe_price_ws(symbol: str):
    global price_ws
    global price_streams

    stream = get_price_stream(symbol)
    if stream not in price_streams:
        return

    if price_ws is not None:
        try:
            await price_ws.unsubscribe([stream])
        except Exception:
            pass

    price_streams.discard(stream)
    if price_ws is not None and not price_streams:
        try:
            await price_ws.close()
        except Exception:
            pass
        price_ws = None


async def sync_tp1_price_streams():
    global tp1_watch

    tp1_watch = {}
    for trade in await db.get_open_trades():
        if trade.take1_triggered or trade.partial_exit_done:
            continue
        tp1_watch[trade.symbol] = TP1Watch(
            trade_id=trade.id,
            side=trade.side,
            take1_price=trade.take1_price,
        )
        await subscribe_price_ws(trade.symbol)


async def clear_tp1_watch(symbol: str):
    tp1_watch.pop(symbol, None)
    _take1_retry_after.pop(symbol, None)
    await unsubscribe_price_ws(symbol)


async def remove_indicator(symbol):
    global indicators
    indicators.pop(symbol, None)


async def ws_error(ws, error):
    print(f"WS ERROR: {error}\n{traceback.format_exc()}")


async def ws_price_msg(ws, msg):
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict):
        return

    symbol = data.get("s")
    bid_price = data.get("b")
    ask_price = data.get("a")
    if not symbol or bid_price is None or ask_price is None:
        return
    if symbol not in tp1_watch:
        return

    await _check_take1_hit(symbol, bid_price=float(bid_price), ask_price=float(ask_price))


async def ws_msg(ws, msg):
    global indicators
    global positions

    if "data" not in msg:
        return

    kline = msg["data"]["k"]
    symbol = kline["s"]
    interval = kline["i"]

    if symbol not in indicators:
        symbol_conf = await db.get_symbol_conf(symbol)
        indicators[symbol] = sloping.Sloping(
            symbol_conf.length,
            symbol_conf.atr_length,
            min_space=symbol_conf.min_space,
            debug=config.getboolean("BOT", "debug"),
            min_touches=symbol_conf.min_touches,
            breakout_buffer=symbol_conf.breakout_buffer,
            slope_filter=symbol_conf.slope_filter,
            use_trend_filter=symbol_conf.use_trend_filter,
            trend_sma=symbol_conf.trend_sma,
            vol_filter=symbol_conf.vol_filter,
            vol_lookback=symbol_conf.vol_lookback,
        )
        klines = await client.klines(symbol, interval)
        for k in klines[:-1]:
            indicators[symbol].add_kline(*k[:5])
        print(f"Индикатор {symbol} создан")

    if kline["x"]:
        if indicators[symbol].add_kline(kline["t"], kline["o"], kline["h"], kline["l"], kline["c"]):
            # --- Trailing stop update on candle close ---
            await update_trailing_stop(symbol, float(kline["h"]), float(kline["l"]))
            # --- Kill switch: перечитываем из БД (conf может быть устаревшим) ---
            fresh_conf = await db.load_config()
            if not fresh_conf.trade_mode:
                return

            # --- Мониторинг TP1 для открытых позиций (только при закрытии свечи) ---
            if positions.get(symbol, False) and symbol in tp1_watch:
                current_price = float(kline["c"])
                await _check_take1_hit(symbol, current_price)

            signal = indicators[symbol].get_value()
            if not positions.get(symbol, False) and signal:
                # Проверка max_positions
                symbol_conf = await db.get_symbol_conf(symbol)
                open_count = await db.get_open_positions_count()
                if open_count >= symbol_conf.max_positions:
                    return

                positions[symbol] = True
                print(f"Открываем позицию по {symbol}")
                asyncio.create_task(new_trade(symbol, signal))


async def _check_take1_hit(symbol, current_price=None, bid_price=None, ask_price=None):
    """Проверить достижение TP1 и запустить частичное закрытие."""
    if symbol in _take1_processing:
        return

    loop = asyncio.get_running_loop()
    if _take1_retry_after.get(symbol, 0.0) > loop.time():
        return

    watch = tp1_watch.get(symbol)
    trade = None
    if watch is None:
        trade = await db.get_open_trade(symbol)
        if not trade or trade.take1_triggered or trade.partial_exit_done:
            return
        watch = TP1Watch(trade_id=trade.id, side=trade.side, take1_price=trade.take1_price)
        tp1_watch[symbol] = watch

    direction_long = watch.side
    trigger_price = None
    if direction_long and bid_price is not None:
        trigger_price = bid_price
    elif (not direction_long) and ask_price is not None:
        trigger_price = ask_price
    elif current_price is not None:
        trigger_price = current_price

    if trigger_price is None:
        return

    take1_hit = (
        trigger_price >= watch.take1_price if direction_long
        else trigger_price <= watch.take1_price
    )

    if take1_hit:
        _take1_retry_after[symbol] = loop.time() + TAKE1_RETRY_COOLDOWN_SEC
        _take1_processing.add(symbol)
        if trade is None:
            trade = await db.get_open_trade(symbol)
        if not trade or trade.id != watch.trade_id or trade.take1_triggered or trade.partial_exit_done:
            _take1_processing.discard(symbol)
            await clear_tp1_watch(symbol)
            return
        print(f"{symbol}: достигнут Take1 @ {trade.take1_price}")
        asyncio.create_task(partial_close_and_move_stop(trade))


async def new_trade(symbol, signal):
    global positions

    # --- Повторная проверка trade_mode (из БД, не из кеша) ---
    fresh_conf = await db.load_config()
    if not fresh_conf.trade_mode:
        positions.pop(symbol, None)
        await clear_tp1_watch(symbol)
        return

    try:
        symbol_info = all_symbols.get(symbol)
        if not symbol_info:
            positions.pop(symbol, None)
            await clear_tp1_watch(symbol)
            return

        symbol_conf = await db.get_symbol_conf(symbol)

        # --- Синхронизируем leverage перед входом ---
        try:
            await client.change_leverage(symbol, symbol_conf.leverage)
        except Exception:
            pass

        # --- Risk-based sizing ---
        try:
            account_info = await client.account()
            balance = float(account_info["totalWalletBalance"])
        except Exception:
            balance = None

        last_price = float((await client.ticker_price(symbol))["price"])
        atr = indicators[symbol].get_atr()

        if balance and atr > 0 and symbol_conf.stop > 0:
            risk_amount = balance * symbol_conf.risk_pct
            stop_distance = atr * symbol_conf.stop
            quantity = utils.round_down(risk_amount / stop_distance, symbol_info.step_size)
        else:
            quantity = utils.round_down(symbol_conf.order_size / last_price, symbol_info.step_size)

        min_notional = symbol_info.notional * 1.1
        if quantity * last_price < min_notional:
            quantity = utils.round_up(min_notional / last_price, symbol_info.step_size)

        entry_order = await client.new_order(
            symbol=symbol,
            side="BUY" if signal.side else "SELL",
            type="MARKET",
            quantity=quantity,
            newOrderRespType="RESULT",
        )
    except Exception:
        print(
            f"Ошибка при открытии {'ЛОНГОВОЙ' if signal.side else 'ШОРТОВОЙ'} позиции по {symbol}\n"
            f"{traceback.format_exc()}"
        )
        positions.pop(symbol, None)
        await clear_tp1_watch(symbol)
        return

    # --- Entry успешен, теперь выставляем exit-ордера ---
    entry_price = float(entry_order["avgPrice"])
    quantity = float(entry_order["executedQty"])

    if signal.side:
        take1_price = entry_price + signal.atr * symbol_conf.take1
        take2_price = entry_price + signal.atr * symbol_conf.take2
        stop_price = entry_price - signal.atr * symbol_conf.stop
    else:
        take1_price = entry_price - signal.atr * symbol_conf.take1
        take2_price = entry_price - signal.atr * symbol_conf.take2
        stop_price = entry_price + signal.atr * symbol_conf.stop

    take1_price = round(take1_price, symbol_info.tick_size)
    take2_price = round(take2_price, symbol_info.tick_size)
    stop_price = round(stop_price, symbol_info.tick_size)

    stop_order = None
    take_order = None
    try:
        stop_order = await client.new_order(
            symbol=symbol,
            side="SELL" if signal.side else "BUY",
            type="STOP_MARKET",
            quantity=quantity,
            stopPrice=stop_price,
            timeInForce="GTE_GTC",
            reduceOnly=True,
        )
        take_order = await client.new_order(
            symbol=symbol,
            side="SELL" if signal.side else "BUY",
            type="LIMIT",
            quantity=quantity,
            price=take2_price,
            timeInForce="GTC",
            reduceOnly=True,
        )
    except Exception:
        print(f"Ошибка при выставлении exit-ордеров по {symbol}\n{traceback.format_exc()}")
        # Аварийное закрытие только если не удалось выставить хотя бы стоп
        if not stop_order:
            try:
                await client.new_order(
                    symbol=symbol,
                    side="SELL" if signal.side else "BUY",
                    type="MARKET",
                    quantity=quantity,
                    reduceOnly=True,
                )
            except Exception:
                print(f"КРИТИЧЕСКАЯ ОШИБКА: не удалось закрыть {symbol}\n{traceback.format_exc()}")
            positions.pop(symbol, None)
            await clear_tp1_watch(symbol)
            return

    # Если take_order не выставлен — обнуляем take2_price чтобы UI не показывал фантомный TP2
    if not take_order:
        take2_price = None
        print(f"{symbol}: TP2 LIMIT не выставлен, сделка защищена только стопом")

    print(
        f"Открыли сделку по {symbol} по цене {entry_price} "
        f"TP1={take1_price} TP2={take2_price} SL={stop_price}"
    )

    # --- Запись в БД (при сбое — аварийное закрытие, чтобы не оставить orphan-позицию) ---
    try:
        async with session() as s:
            trade = db.Trades(
                symbol=symbol,
                order_size=float(entry_order["cumQuote"]),
                side=signal.side,
                status="NEW",
                position_open=True,
                open_time=entry_order["updateTime"],
                interval=symbol_conf.interval,
                window_size=symbol_conf.length,
                leverage=symbol_conf.leverage,
                atr_length=symbol_conf.atr_length,
                atr=signal.atr,
                take1_atr=symbol_conf.take1,
                take2_atr=symbol_conf.take2,
                loss_atr=symbol_conf.stop,
                entry_price=entry_price,
                quantity=quantity,
                take1_price=take1_price,
                take2_price=take2_price,
                stop_price=stop_price,
            )
            s.add(trade)
            await s.commit()

            orders_to_save = [entry_order]
            if stop_order:
                orders_to_save.append(stop_order)
            if take_order:
                orders_to_save.append(take_order)

            for order in orders_to_save:
                s.add(
                    db.Orders(
                        order_id=order["orderId"],
                        trade_id=trade.id,
                        symbol=symbol,
                        time=order["updateTime"],
                        side=order["side"] == "BUY",
                        type=order["type"],
                        status=order["status"],
                        reduce=order["reduceOnly"],
                        price=float(order["avgPrice"]),
                        quantity=float(order["executedQty"]),
                    )
                )
            await s.commit()
    except Exception:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: БД недоступна по {symbol}, закрываем позицию\n{traceback.format_exc()}")
        # Отменяем exit-ордера и закрываем позицию, чтобы не оставить orphan
        try:
            await client.cancel_open_orders(symbol=symbol)
        except Exception:
            pass
        try:
            await client.new_order(
                symbol=symbol,
                side="SELL" if signal.side else "BUY",
                type="MARKET",
                quantity=quantity,
                reduceOnly=True,
            )
        except Exception:
            print(f"КРИТИЧЕСКАЯ ОШИБКА: не удалось закрыть orphan-позицию {symbol}\n{traceback.format_exc()}")
        positions.pop(symbol, None)
        await clear_tp1_watch(symbol)
        return

    # --- Telegram-уведомление (падение не влияет на сделку) ---
    tp1_watch[symbol] = TP1Watch(
        trade_id=trade.id,
        side=signal.side,
        take1_price=take1_price,
    )
    try:
        await subscribe_price_ws(symbol)
    except Exception:
        print(f"{symbol}: intrabar TP1 monitor was not attached\n{traceback.format_exc()}")

    try:
        text = (
            f"Открыл в <b>{'ЛОНГ' if signal.side else 'ШОРТ'}</b> {quantity} <b>{symbol}</b>\n"
            f"Цена входа: <b>{entry_price}</b>\n"
            f"Тейк 1 / Тейк 2: <b>{take1_price} / {take2_price}</b>\n"
            f"Стоп: <b>{stop_price}</b>\n"
        )
        msg = await tg.bot.send_message(config["TG"]["channel"], text, parse_mode="HTML")
        async with session() as s:
            await s.execute(
                sa_update(db.Trades).where(db.Trades.id == trade.id).values(msg_id=msg.message_id)
            )
            await s.commit()
    except Exception:
        print(f"Telegram уведомление не отправлено по {symbol}\n{traceback.format_exc()}")


async def update_trailing_stop(symbol: str, bar_high: float, bar_low: float):
    """On each candle close, update trailing stop if trail_after_tp1 is enabled."""
    trade = await db.get_open_trade(symbol)
    if not trade or not trade.partial_exit_done or trade.trail_high is None:
        return

    symbol_conf = await db.get_symbol_conf(symbol)
    if not symbol_conf.trail_after_tp1:
        return

    symbol_info = all_symbols.get(symbol)
    if not symbol_info:
        return

    trail_distance = trade.atr * symbol_conf.trail_atr
    direction_long = trade.side

    if direction_long:
        new_trail_high = max(trade.trail_high, bar_high)
        new_stop = new_trail_high - trail_distance
    else:
        new_trail_high = min(trade.trail_high, bar_low)
        new_stop = new_trail_high + trail_distance

    new_stop = round(new_stop, symbol_info.tick_size)
    old_stop = trade.stop_price or trade.breakeven_stop_price

    # Only move stop if it improved
    improved = (direction_long and new_stop > old_stop) or (not direction_long and new_stop < old_stop)
    if not improved and new_trail_high == trade.trail_high:
        return

    # Update trail_high in DB even if stop hasn't moved yet
    async with session() as s:
        await s.execute(
            sa_update(db.Trades).where(db.Trades.id == trade.id).values(
                trail_high=new_trail_high,
            )
        )
        await s.commit()

    if not improved:
        return

    # Re-place stop order on exchange
    close_side = "SELL" if direction_long else "BUY"
    step_size = symbol_info.step_size

    try:
        binance_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binance_positions if p["symbol"] == symbol), None)
        if not position_info or abs(float(position_info["positionAmt"])) == 0:
            return
        remaining_qty = utils.round_down(abs(float(position_info["positionAmt"])), step_size)

        # Cancel old stop orders
        for order in await db.get_trade_orders(trade.id):
            if order.type == "STOP_MARKET" and order.status not in ("FILLED", "CANCELLED", "CANCELED", "EXPIRED"):
                try:
                    await client.cancel_order(symbol=symbol, orderId=order.order_id)
                except Exception:
                    pass

        # Place new trailing stop
        new_stop_order = await client.new_order(
            symbol=symbol,
            side=close_side,
            type="STOP_MARKET",
            stopPrice=new_stop,
            quantity=remaining_qty,
            reduceOnly=True,
        )
        print(f"{symbol}: trailing stop moved -> {new_stop}")

        async with session() as s:
            s.add(db.Orders(
                order_id=new_stop_order["orderId"],
                trade_id=trade.id,
                symbol=symbol,
                time=new_stop_order["updateTime"],
                side=new_stop_order["side"] == "BUY",
                type=new_stop_order["type"],
                status=new_stop_order["status"],
                reduce=True,
                price=0.0,
                quantity=remaining_qty,
            ))
            await s.execute(
                sa_update(db.Trades).where(db.Trades.id == trade.id).values(
                    stop_price=new_stop,
                )
            )
            await s.commit()

    except Exception:
        print(f"{symbol}: trailing stop update error\n{traceback.format_exc()}")


async def partial_close_and_move_stop(trade):
    """Частичное закрытие позиции по TP1 + перенос стопа в безубыток."""
    global positions

    symbol = trade.symbol
    symbol_conf = await db.get_symbol_conf(symbol)
    symbol_info = all_symbols.get(symbol)
    if not symbol_info:
        _take1_processing.discard(symbol)
        await clear_tp1_watch(symbol)
        return

    portion = symbol_conf.portion
    step_size = symbol_info.step_size
    min_qty = symbol_info.min_qty
    tick_size = symbol_info.tick_size
    entry_price = trade.entry_price
    direction_long = trade.side
    close_side = "SELL" if direction_long else "BUY"

    if trade.partial_exit_done:
        await clear_tp1_watch(symbol)
        print(f"{symbol}: частичное закрытие уже выполнено, пропуск.")
        return

    # --- Шаг 1: Закрыть portion позиции рыночным ордером ---
    try:
        binance_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binance_positions if p["symbol"] == symbol), None)

        if not position_info or abs(float(position_info["positionAmt"])) == 0:
            await clear_tp1_watch(symbol)
            print(f"{symbol}: позиция уже закрыта.")
            _take1_processing.discard(symbol)
            return

        qty = utils.round_down(trade.quantity * portion, step_size)

        if qty < min_qty:
            await clear_tp1_watch(symbol)
            print(f"{symbol}: объём {qty} < min_qty {min_qty}, пропускаем частичное закрытие.")
            _take1_processing.discard(symbol)
            return

        await client.new_order(
            symbol=symbol,
            side=close_side,
            type="MARKET",
            quantity=qty,
            reduceOnly=True,
        )
        print(f"{symbol}: частично закрыто {portion * 100:.0f}% позиции ({qty})")

    except Exception:
        print(f"{symbol}: ошибка при частичном MARKET-закрытии\n{traceback.format_exc()}")
        _take1_processing.discard(symbol)
        return

    # --- Шаг 2: Проверить остаток и переставить ордера ---
    try:
        binance_positions = await client.get_position_risk(symbol=symbol)
        position_info = next((p for p in binance_positions if p["symbol"] == symbol), None)
        remaining_amt = abs(float(position_info["positionAmt"]))

        if remaining_amt == 0:
            await clear_tp1_watch(symbol)
            print(f"{symbol}: позиция полностью закрыта после частичного выхода.")
            positions.pop(symbol, None)
            _take1_processing.discard(symbol)
            return

        remaining_qty = utils.round_down(remaining_amt, step_size)

        # Безубыток: entry_price ± 0.1% (чуть ниже/выше entry для покрытия комиссий)
        new_stop = round(
            entry_price * (0.999 if direction_long else 1.001),
            tick_size,
        )

        # --- Отменяем все старые стоп/тейк ордера ---
        for order in await db.get_trade_orders(trade.id):
            if not order.reduce or order.status in ("FILLED", "CANCELLED", "EXPIRED", "CANCELED"):
                continue
            try:
                await client.cancel_order(symbol=symbol, orderId=order.order_id)
            except Exception:
                pass

        # --- Новый стоп на безубыток ---
        new_stop_order = await client.new_order(
            symbol=symbol,
            side=close_side,
            type="STOP_MARKET",
            stopPrice=new_stop,
            quantity=remaining_qty,
            reduceOnly=True,
        )
        print(f"{symbol}: новый стоп-БУ @ {new_stop}, объём {remaining_qty}")

        # Сохраняем стоп-ордер в БД
        async with session() as s:
            s.add(db.Orders(
                order_id=new_stop_order["orderId"],
                trade_id=trade.id,
                symbol=symbol,
                time=new_stop_order["updateTime"],
                side=new_stop_order["side"] == "BUY",
                type=new_stop_order["type"],
                status=new_stop_order["status"],
                reduce=True,
                price=0.0,
                quantity=remaining_qty,
            ))
            await s.commit()

        # --- Новый тейк2 на оставшийся объём (только если TP2 был выставлен) ---
        if trade.take2_price:
            new_take_order = await client.new_order(
                symbol=symbol,
                side=close_side,
                type="LIMIT",
                price=trade.take2_price,
                quantity=remaining_qty,
                timeInForce="GTC",
                reduceOnly=True,
            )
            print(f"{symbol}: тейк2 обновлён @ {trade.take2_price}, объём {remaining_qty}")

            # Сохраняем тейк-ордер в БД
            async with session() as s:
                s.add(db.Orders(
                    order_id=new_take_order["orderId"],
                    trade_id=trade.id,
                    symbol=symbol,
                    time=new_take_order["updateTime"],
                    side=new_take_order["side"] == "BUY",
                    type=new_take_order["type"],
                    status=new_take_order["status"],
                    reduce=True,
                    price=float(new_take_order.get("price", trade.take2_price)),
                    quantity=remaining_qty,
                ))
                await s.commit()
        else:
            print(f"{symbol}: TP2 отсутствует, пропускаем выставление LIMIT")

        # --- Обновляем trade в БД ---
        trail_high_val = float(position_info["markPrice"]) if symbol_conf.trail_after_tp1 else None
        async with session() as s:
            await s.execute(
                sa_update(db.Trades).where(db.Trades.id == trade.id).values(
                    breakeven_stop_price=new_stop,
                    partial_exit_done=True,
                    take1_triggered=True,
                    trail_high=trail_high_val,
                )
            )
            await s.commit()

        _take1_processing.discard(symbol)
        await clear_tp1_watch(symbol)

        text = (
            f"<b>{symbol}</b>: частично закрыта позиция <b>{'ЛОНГ' if direction_long else 'ШОРТ'}</b>\n"
            f"🛡️ Стоп перенесён в БУ: <b>{new_stop}</b>\n"
            f"🎯 Тейк2: <b>{trade.take2_price}</b>, объём: <b>{remaining_qty}</b>"
        )
        try:
            if trade.msg_id:
                await tg.bot.send_message(
                    config["TG"]["channel"],
                    text,
                    reply_to_message_id=trade.msg_id,
                    parse_mode="HTML",
                )
        except Exception:
            pass

    except Exception:
        print(f"{symbol}: ошибка после частичного закрытия\n{traceback.format_exc()}")
        _take1_processing.discard(symbol)


async def cancel_trade_exit_orders(symbol, trade_id, filled_order_id):
    for exit_order in await db.get_trade_orders(trade_id):
        if exit_order.order_id == filled_order_id or not exit_order.reduce:
            continue
        if exit_order.status in ("FILLED", "CANCELLED", "EXPIRED", "CANCELED"):
            continue
        try:
            await client.cancel_order(symbol=symbol, orderId=exit_order.order_id)
        except Exception:
            pass


async def ws_user_msg(ws, msg):
    global positions

    event_type = msg.get("e") if isinstance(msg, dict) else None

    # --- ACCOUNT_UPDATE: обновление позиций ---
    if event_type == "ACCOUNT_UPDATE":
        for pos in msg["a"]["P"]:
            sym = pos["s"]
            position_amt = float(pos["pa"])
            if position_amt != 0.0:
                positions[sym] = True
            else:
                positions.pop(sym, None)
        return

    if event_type != "ORDER_TRADE_UPDATE":
        return

    o = msg["o"]
    if not (o.get("R") and o["X"] in ("FILLED", "PARTIALLY_FILLED", "CANCELLED", "CANCELED", "EXPIRED")):
        return

    symbol = o["s"]
    realized_profit = float(o.get("rp") or 0.0)
    price = float(o.get("ap") or 0.0)
    quantity = float(o.get("z") or 0.0)
    order_id = o["i"]
    status = o["X"]

    order, trade = await db.get_order_trade(order_id)

    if not order:
        trade = await db.get_open_trade(symbol)
        if not trade:
            trade = await db.get_last_trade(symbol)
        if trade:
            async with session() as s:
                order = db.Orders(
                    order_id=order_id,
                    trade_id=trade.id,
                    symbol=symbol,
                    time=o["T"],
                    side=o["S"] == "BUY",
                    type=o["o"],
                    status="NEW",
                    reduce=o["R"],
                    price=price,
                    quantity=quantity,
                    realized_profit=realized_profit,
                )
                s.add(order)
                await s.commit()

    if not order or not trade:
        return

    order.status = status
    order.time = o["T"]
    if price:
        order.price = price
    if quantity:
        order.quantity = quantity
    order.realized_profit = realized_profit

    await db.update_order_trade(order, trade)
    await db.update_trade_result(trade.id)

    # --- Проверяем, закрыта ли позиция (через биржу, а не через positions dict) ---
    if status == "FILLED" and trade.position_open:
        try:
            pos_info = await client.get_position_risk(symbol=symbol)
            actual_pos = next((p for p in pos_info if p["symbol"] == symbol), None)
            is_position_closed = actual_pos and float(actual_pos["positionAmt"]) == 0
        except Exception:
            # Фолбэк на positions dict если API недоступен
            is_position_closed = not positions.get(symbol, False)

        if not is_position_closed:
            return

        # Перечитываем trade из БД чтобы избежать гонки при двух одновременных exit-ордерах
        fresh_trade = await db.get_open_trade(symbol)
        if not fresh_trade or not fresh_trade.position_open:
            return

        trade.close_time = o["T"]
        trade.position_open = False

        ot = o.get("ot")
        if ot == "STOP_MARKET":
            trade.status = (
                "CLOSED_BREAKEVEN" if trade.take1_triggered and trade.breakeven_stop_price
                else "CLOSED_STOP"
            )
        elif ot == "LIMIT":
            trade.status = "CLOSED_TAKE"
        else:
            trade.status = "CLOSED_MARKET"

        await cancel_trade_exit_orders(symbol, trade.id, order_id)
        await db.update_order_trade(order, trade)
        await db.update_trade_result(trade.id)

        # Обновляем итоговый результат из БД
        updated_trade = await db.get_last_trade(symbol)
        final_result = updated_trade.result if updated_trade else trade.result

        status_text = {
            "CLOSED_BREAKEVEN": "🔄 стоп в БУ (после Take1)",
            "CLOSED_STOP": "⛔️ по СТОПУ",
            "CLOSED_TAKE": "🎯 по Take2",
            "CLOSED_MARKET": "📉 по рынку",
        }.get(trade.status, "📉 закрыта")

        text = (
            f"Сделка по паре #{symbol} в <b>{'ЛОНГ' if trade.side else 'ШОРТ'}</b> закрыта {status_text}\n"
            f"{'Прибыль' if final_result > 0 else 'Убыток'}: <b>{round(abs(final_result), 4)} USDT</b>\n"
        )
        try:
            await tg.bot.send_message(
                config["TG"]["channel"],
                text,
                reply_to_message_id=trade.msg_id,
                parse_mode="HTML",
            )
        except Exception:
            pass

        await clear_tp1_watch(symbol)
        positions.pop(symbol, None)
        print(f"Сделка по {symbol} закрыта ({trade.status})")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(BASE_DIR / "config.ini")
    asyncio.run(main())
