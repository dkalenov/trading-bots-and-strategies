import asyncio
import time
from collections import deque

import binance


# Latest selected support density per symbol:
# symbol -> (last_seen_bid_price, last_seen_bid_price_str, peak_bid_volume)
last_density = {}
last_density_emit = {}
rolling_depth = {}  # symbol -> { 'bid': float, 'ask': float }
DENSITY_HEARTBEAT_SECONDS = 0.5
max_density_level = 7

msg_count = 0
last_log_time = 0.0

# v9 order flow tracking globals
trades_window = {}      # symbol -> deque of (t_time, price, qty, is_buyer_maker)
active_densities = {}   # symbol -> dict of active density stats
history_densities = {}  # symbol -> dict of inactive density stats (flicker protection)


def run(_symbols, _depth, _ask_volume, _main_queue, testnet, _max_density_level=7):
    global symbols
    global depth
    global ask_volume
    global main_queue
    global max_density_level
    symbols = _symbols
    depth = _depth
    ask_volume = _ask_volume
    main_queue = _main_queue
    max_density_level = _max_density_level
    print(f"Запущен процесс вебсокета для следующих пар: {', '.join([sym.symbol for sym in symbols])}", flush=True)
    asyncio.run(main(testnet))


async def main(testnet=False):
    global client
    global websocket
    client = binance.Futures(asynced=True, testnet=testnet)
    streams = []
    for sym in symbols:
        streams.append(f"{sym.symbol.lower()}@depth{depth}@100ms")
        streams.append(f"{sym.symbol.lower()}@aggTrade")
    websocket = await client.websocket(streams, on_message=on_message, on_error=ws_error)
    while websocket.working:
        await asyncio.sleep(1)


async def ws_error(ws, error):
    print(f"ws_error: {error}", flush=True)


def _level_value(level):
    return float(level[0]) * float(level[1])


def _sum_value(levels, limit=5):
    return sum(_level_value(level) for level in levels[:limit])


def _trade_flow(symbol):
    buy_vol_5s = 0.0
    sell_vol_5s = 0.0
    if symbol in trades_window:
        for t in trades_window[symbol]:
            if t[3]:  # True = sell market order
                sell_vol_5s += t[2]
            else:
                buy_vol_5s += t[2]
    if sell_vol_5s > 0:
        delta_ratio = buy_vol_5s / sell_vol_5s
    elif buy_vol_5s > 0:
        delta_ratio = 999.0
    else:
        delta_ratio = 1.0
    return buy_vol_5s, sell_vol_5s, delta_ratio


def _book_metrics(bids, asks):
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    bid_qty_0 = float(bids[0][1])
    ask_qty_0 = float(asks[0][1])
    mid = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else best_bid
    spread_pct = ((best_ask - best_bid) / best_bid * 100.0) if best_bid > 0 else 0.0

    top_bid_value = _sum_value(bids, 5)
    top_ask_value = _sum_value(asks, 5)
    total_top_value = top_bid_value + top_ask_value
    imbalance = ((top_bid_value - top_ask_value) / total_top_value) if total_top_value > 0 else 0.0

    if bid_qty_0 + ask_qty_0 > 0 and mid > 0:
        microprice = (best_ask * bid_qty_0 + best_bid * ask_qty_0) / (bid_qty_0 + ask_qty_0)
        microprice_edge_pct = (microprice - mid) / mid * 100.0
    else:
        microprice_edge_pct = 0.0

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_pct": spread_pct,
        "top_bid_value": top_bid_value,
        "top_ask_value": top_ask_value,
        "imbalance": imbalance,
        "microprice_edge_pct": microprice_edge_pct,
    }


def _score_bid_candidate(num, bid_price, bid_val_usd, dynamic_min_volume, metrics, buy_vol_5s, sell_vol_5s, delta_ratio):
    density_ratio = bid_val_usd / max(dynamic_min_volume, 1.0)
    wall_to_ask_ratio = bid_val_usd / max(metrics["top_ask_value"], 1.0)
    distance_pct = max((metrics["best_bid"] - bid_price) / metrics["best_bid"] * 100.0, 0.0)

    score = 0.0
    score += min(4.0, density_ratio * 1.6)
    score += min(3.0, wall_to_ask_ratio * 2.0)

    if 0.01 <= distance_pct <= 0.35:
        score += 2.0
    elif distance_pct <= 0.60:
        score += 0.75
    else:
        score -= 3.0

    if metrics["imbalance"] >= 0.15:
        score += 2.0
    elif metrics["imbalance"] >= 0.03:
        score += 0.75
    elif metrics["imbalance"] <= -0.15:
        score -= 2.0

    if metrics["microprice_edge_pct"] >= 0.006:
        score += 1.5
    elif metrics["microprice_edge_pct"] <= -0.006:
        score -= 1.5

    if delta_ratio >= 1.2:
        score += 1.5
    elif delta_ratio <= 0.55 and sell_vol_5s > buy_vol_5s:
        score -= 2.0

    if metrics["spread_pct"] <= 0.05:
        score += 1.0
    elif metrics["spread_pct"] >= 0.12:
        score -= 1.0

    score -= min(num, 10) * 0.15
    return {
        "opportunity_score": round(score, 3),
        "density_ratio": density_ratio,
        "wall_to_ask_ratio": wall_to_ask_ratio,
        "distance_pct": distance_pct,
    }


def _park_active_density(symbol, current_time):
    active_d = active_densities.get(symbol)
    if not active_d:
        return
    history_densities[symbol] = {
        "price": active_d["price"],
        "first_seen_ts": active_d["first_seen_ts"],
        "refill_count": active_d["refill_count"],
        "refill_volume": active_d["refill_volume"],
        "initial_volume": active_d["initial_volume"],
        "absorbed_sell_volume": active_d["absorbed_sell_volume"],
        "inactive_ts": current_time,
    }


def _emit_density(symbol, price, volume, ask_vol, num, event_time, metrics,
                  lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                  buy_vol_5s, sell_vol_5s, opportunity):
    main_queue.put((
        symbol,
        price,
        volume,
        ask_vol,
        num,
        time.perf_counter_ns(),
        event_time,
        metrics["best_bid"],
        metrics["best_ask"],
        lifetime_sec,
        refill_count,
        refill_ratio,
        absorption_ratio,
        buy_vol_5s,
        sell_vol_5s,
        opportunity.get("opportunity_score", 0.0),
        metrics.get("imbalance", 0.0),
        metrics.get("microprice_edge_pct", 0.0),
        metrics.get("spread_pct", 0.0),
        opportunity.get("distance_pct", 0.0),
        opportunity.get("density_ratio", 0.0),
        opportunity.get("wall_to_ask_ratio", 0.0),
        opportunity.get("delta_ratio", 1.0),
    ))


async def on_message(ws, msg):
    global last_density, last_density_emit, msg_count, last_log_time, rolling_depth
    global trades_window, active_densities, history_densities
    msg_count += 1
    current_time = time.time()
    if last_log_time == 0.0:
        last_log_time = current_time
    elif current_time - last_log_time >= 60.0:
        proc_id = symbols[0].symbol if symbols else "Unknown"
        print(f"[WebSocket {proc_id}] Обработано {msg_count} обновлений за 60 сек (в среднем {round(msg_count / 60.0, 1)}/сек)", flush=True)
        msg_count = 0
        last_log_time = current_time

    if "data" not in msg:
        return

    stream = msg.get("stream", "")
    data = msg["data"]
    symbol = stream.split("@")[0].upper()

    if "aggtrade" in stream.lower():
        p = float(data["p"])
        q = float(data["q"])
        is_buyer_maker = bool(data["m"])  # True = sell market order, False = buy market order
        t_time = float(data["T"]) / 1000.0

        if symbol not in trades_window:
            trades_window[symbol] = deque()
        trades_window[symbol].append((t_time, p, q, is_buyer_maker))

        while trades_window[symbol] and current_time - trades_window[symbol][0][0] > 5.0:
            trades_window[symbol].popleft()

        active_d = active_densities.get(symbol)
        if active_d is not None:
            density_price = active_d["price"]
            if density_price * 0.9995 <= p <= density_price * 1.0005:
                if is_buyer_maker:
                    active_d["absorbed_sell_volume"] += q
        return

    if "depth" not in stream.lower():
        return

    bids = data.get("bids") or data.get("b")
    asks = data.get("asks") or data.get("a")
    if not bids or not asks:
        return

    ask_vol = sum(float(ask[1]) for ask in asks)
    bid_vol = sum(float(bid[1]) for bid in bids)
    if ask_vol <= 0 or bid_vol <= 0:
        return

    avg_ask_vol = ask_vol / len(asks)
    avg_bid_vol = bid_vol / len(bids)
    top_bids = bids[:10]
    top_asks = asks[:10]
    current_avg_bid_val = sum(_level_value(b) for b in top_bids) / len(top_bids) if top_bids else 0.0
    current_avg_ask_val = sum(_level_value(a) for a in top_asks) / len(top_asks) if top_asks else 0.0

    if symbol not in rolling_depth:
        rolling_depth[symbol] = {"bid": current_avg_bid_val, "ask": current_avg_ask_val}
    else:
        alpha = 0.01
        rolling_depth[symbol]["bid"] = rolling_depth[symbol]["bid"] * (1 - alpha) + current_avg_bid_val * alpha
        rolling_depth[symbol]["ask"] = rolling_depth[symbol]["ask"] * (1 - alpha) + current_avg_ask_val * alpha

    metrics = _book_metrics(bids, asks)
    buy_vol_5s, sell_vol_5s, delta_ratio = _trade_flow(symbol)

    candidates = []
    for num, bid in enumerate(bids):
        if num < 1 or num > max_density_level:
            continue
        bid_volume = float(bid[1])
        bid_price = float(bid[0])
        bid_val_usd = bid_price * bid_volume

        local_ratio = bid_volume / max(avg_ask_vol, avg_bid_vol, 1e-12)
        if local_ratio < ask_volume:
            continue

        historical_min_volume = rolling_depth[symbol]["bid"] * ask_volume
        dynamic_min_volume = max(historical_min_volume, 20000.0)
        if bid_val_usd < dynamic_min_volume:
            continue

        opportunity = _score_bid_candidate(
            num, bid_price, bid_val_usd, dynamic_min_volume, metrics,
            buy_vol_5s, sell_vol_5s, delta_ratio
        )
        opportunity["delta_ratio"] = delta_ratio
        candidates.append({
            "num": num,
            "price": bid_price,
            "price_str": bid[0],
            "volume": bid_volume,
            "value_usd": bid_val_usd,
            "local_ratio": local_ratio,
            "dynamic_min_volume": dynamic_min_volume,
            "opportunity": opportunity,
        })

    if candidates:
        best = max(candidates, key=lambda c: c["opportunity"]["opportunity_score"])
        bid_price = best["price"]
        bid_price_str = best["price_str"]
        bid_volume = best["volume"]

        active_d = active_densities.get(symbol)
        if active_d is not None and active_d["price"] == bid_price:
            prev_vol = active_d["last_volume"]
            if bid_volume > prev_vol * 1.20:
                active_d["refill_count"] += 1
                active_d["refill_volume"] += bid_volume - prev_vol
                print(f"[Refill Detected] {symbol} @ {bid_price}: {prev_vol} -> {bid_volume} (+{round(bid_volume - prev_vol, 2)})", flush=True)
            active_d["last_volume"] = bid_volume
            active_d["last_seen_ts"] = current_time
        else:
            if active_d is not None:
                _park_active_density(symbol, current_time)
            hist = history_densities.get(symbol)
            if hist and hist["price"] == bid_price and current_time - hist["inactive_ts"] <= 2.0:
                active_densities[symbol] = {
                    "price": bid_price,
                    "first_seen_ts": hist["first_seen_ts"],
                    "refill_count": hist["refill_count"],
                    "refill_volume": hist["refill_volume"],
                    "initial_volume": hist["initial_volume"],
                    "last_volume": bid_volume,
                    "absorbed_sell_volume": hist["absorbed_sell_volume"],
                    "last_seen_ts": current_time,
                }
                print(f"[Flicker Recovery] {symbol} @ {bid_price} - restored lifetime clock ({round(current_time - hist['first_seen_ts'], 1)}s)", flush=True)
            else:
                active_densities[symbol] = {
                    "price": bid_price,
                    "first_seen_ts": current_time,
                    "refill_count": 0,
                    "refill_volume": 0.0,
                    "initial_volume": bid_volume,
                    "last_volume": bid_volume,
                    "absorbed_sell_volume": 0.0,
                    "last_seen_ts": current_time,
                }
            history_densities.pop(symbol, None)

        active_d = active_densities[symbol]
        lifetime_sec = current_time - active_d["first_seen_ts"]
        refill_count = active_d["refill_count"]
        refill_ratio = active_d["refill_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0
        absorption_ratio = active_d["absorbed_sell_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0

        if symbol in last_density and last_density[symbol] is not None and last_density[symbol][1] == bid_price_str:
            last_price_float, last_price_str, peak_vol = last_density[symbol]
            last_density[symbol] = (last_price_float, last_price_str, max(peak_vol, bid_volume))
            if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                last_density_emit[symbol] = current_time
                _emit_density(
                    symbol, bid_price, bid_volume, ask_vol, best["num"], data.get("E"), metrics,
                    lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                    buy_vol_5s, sell_vol_5s, best["opportunity"]
                )
            return

        print(
            f"Обнаружена alpha-плотность {symbol} цена {bid_price_str.rstrip('0')}, "
            f"объем ${round(best['value_usd'], 2)}, score={best['opportunity']['opportunity_score']}, "
            f"level={best['num']}, dist={best['opportunity']['distance_pct']:.3f}%, "
            f"imb={metrics['imbalance']:.2f}, micro={metrics['microprice_edge_pct']:.4f}%",
            flush=True
        )
        last_density[symbol] = (bid_price, bid_price_str, bid_volume)
        last_density_emit[symbol] = current_time
        _emit_density(
            symbol, bid_price, bid_volume, ask_vol, best["num"], data.get("E"), metrics,
            lifetime_sec, refill_count, refill_ratio, absorption_ratio,
            buy_vol_5s, sell_vol_5s, best["opportunity"]
        )
        return

    if symbol not in last_density or last_density[symbol] is None:
        return

    last_price_float, last_price_str, peak_vol = last_density[symbol]
    still_exists = False
    for old_num, bid in enumerate(bids):
        if bid[0] == last_price_str:
            bid_volume = float(bid[1])
            if bid_volume >= peak_vol * 0.5 and last_price_float * bid_volume >= 8000.0:
                still_exists = True
                last_density[symbol] = (last_price_float, last_price_str, max(peak_vol, bid_volume))

                active_d = active_densities.get(symbol)
                if active_d is not None:
                    prev_vol = active_d["last_volume"]
                    if bid_volume > prev_vol * 1.20:
                        active_d["refill_count"] += 1
                        active_d["refill_volume"] += bid_volume - prev_vol
                    active_d["last_volume"] = bid_volume
                    active_d["last_seen_ts"] = current_time

                    lifetime_sec = current_time - active_d["first_seen_ts"]
                    refill_count = active_d["refill_count"]
                    refill_ratio = active_d["refill_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0
                    absorption_ratio = active_d["absorbed_sell_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0
                else:
                    lifetime_sec = 0.0
                    refill_count = 0
                    refill_ratio = 0.0
                    absorption_ratio = 0.0

                opportunity = {
                    "opportunity_score": 0.0,
                    "distance_pct": max((metrics["best_bid"] - last_price_float) / metrics["best_bid"] * 100.0, 0.0),
                    "density_ratio": 0.0,
                    "wall_to_ask_ratio": 0.0,
                    "delta_ratio": delta_ratio,
                }
                if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                    last_density_emit[symbol] = current_time
                    _emit_density(
                        symbol, last_price_float, bid_volume, ask_vol, old_num, data.get("E"), metrics,
                        lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                        buy_vol_5s, sell_vol_5s, opportunity
                    )
            break

    if still_exists:
        return

    print(f"Плотность по паре {symbol} на цене {last_price_str.rstrip('0')} исчезла или сильно уменьшилась! Отправляем мгновенный сигнал отмены.", flush=True)
    _park_active_density(symbol, current_time)
    active_densities[symbol] = None
    last_density[symbol] = None
    last_density_emit.pop(symbol, None)
    _emit_density(
        symbol, None, 0.0, 0.0, -1, data.get("E"), metrics,
        0.0, 0, 0.0, 0.0, 0.0, 0.0,
        {"opportunity_score": 0.0, "distance_pct": 0.0, "density_ratio": 0.0, "wall_to_ask_ratio": 0.0, "delta_ratio": 1.0}
    )
