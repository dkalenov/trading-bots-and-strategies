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

# v13: Absorption tracking
# Track sell waves hitting the density to prove it's real support
ABSORPTION_WAVE_TIMEOUT = 8.0   # seconds of silence to end a sell wave
BOUNCE_CONFIRM_WINDOW = 5.0     # seconds to confirm bounce after absorption


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


def _score_bid_candidate_v13(num, bid_price, bid_val_usd, dynamic_min_volume, metrics,
                              buy_vol_5s, sell_vol_5s, delta_ratio,
                              absorption_ratio, refill_count, lifetime_sec):
    """v13 scoring: focused on proven absorption and density quality."""
    distance_pct = max((metrics["best_bid"] - bid_price) / metrics["best_bid"] * 100.0, 0.0)

    score = 0.0

    # 1. Absorption Quality (0-5 points) - THE key signal
    #    How much sell volume was absorbed by this density
    absorption_score = min(5.0, absorption_ratio * 5.0)
    score += absorption_score

    # 2. Refill Conviction (0-4 points)
    #    Density being refilled proves real interest, not spoof
    if refill_count >= 3:
        refill_score = 4.0
    elif refill_count >= 2:
        refill_score = 3.0
    elif refill_count >= 1:
        refill_score = 1.5
    else:
        refill_score = 0.0
    score += refill_score

    # 3. Lifetime (0-3 points)
    #    Longer = less likely spoofing
    if lifetime_sec >= 30.0:
        lifetime_score = 3.0
    elif lifetime_sec >= 20.0:
        lifetime_score = 2.5
    elif lifetime_sec >= 15.0:
        lifetime_score = 2.0
    elif lifetime_sec >= 10.0:
        lifetime_score = 1.0
    else:
        lifetime_score = 0.0
    score += lifetime_score

    # 4. Distance from best bid (0-2 points)
    #    Closer = more likely to be tested
    if distance_pct <= 0.10:
        score += 2.0
    elif distance_pct <= 0.20:
        score += 1.0
    elif distance_pct <= 0.40:
        score += 0.5
    # else: no points

    # 5. Book Imbalance bonus/penalty (-1 to +2)
    if metrics["imbalance"] >= 0.15:
        score += 2.0
    elif metrics["imbalance"] >= 0.05:
        score += 1.0
    elif metrics["imbalance"] <= -0.15:
        score -= 1.0

    # 6. Microprice edge (-1 to +1)
    if metrics["microprice_edge_pct"] >= 0.006:
        score += 1.0
    elif metrics["microprice_edge_pct"] <= -0.006:
        score -= 1.0

    # 7. Trade flow delta (-1 to +1)
    if delta_ratio >= 1.3:
        score += 1.0
    elif delta_ratio <= 0.5 and sell_vol_5s > buy_vol_5s:
        score -= 1.0

    # 8. Spread penalty
    if metrics["spread_pct"] >= 0.10:
        score -= 1.0

    # 9. Level penalty (deeper = worse)
    score -= min(num, 5) * 0.3

    return {
        "opportunity_score": round(score, 3),
        "absorption_ratio": round(absorption_ratio, 4),
        "refill_count": refill_count,
        "lifetime_sec": round(lifetime_sec, 1),
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
        "bounce_buy_volume": active_d.get("bounce_buy_volume", 0.0),
        "bounce_confirmed": active_d.get("bounce_confirmed", False),
        "sell_wave_volume": active_d.get("sell_wave_volume", 0.0),
        "sell_wave_active": active_d.get("sell_wave_active", False),
        "inactive_ts": current_time,
    }


def _emit_density(symbol, price, volume, ask_vol, num, event_time, metrics,
                  lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                  buy_vol_5s, sell_vol_5s, opportunity,
                  bounce_buy_volume=0.0, bounce_confirmed=False):
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
        opportunity.get("absorption_ratio", 0.0),
        opportunity.get("refill_count", 0),
        opportunity.get("lifetime_sec", 0.0),
        bounce_buy_volume,
        bounce_confirmed,
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

    # ── AGGTRADE: track sell absorption and bounce confirmation ──
    if "aggtrade" in stream.lower():
        p = float(data["p"])
        q = float(data["q"])
        is_buyer_maker = bool(data["m"])  # True = sell market order
        t_time = float(data["T"]) / 1000.0

        if symbol not in trades_window:
            trades_window[symbol] = deque()
        trades_window[symbol].append((t_time, p, q, is_buyer_maker))

        while trades_window[symbol] and current_time - trades_window[symbol][0][0] > 5.0:
            trades_window[symbol].popleft()

        active_d = active_densities.get(symbol)
        if active_d is not None:
            density_price = active_d["price"]

            # ── v13: Track sell waves hitting the density ──
            if density_price * 0.9997 <= p <= density_price * 1.0003:
                if is_buyer_maker:
                    # Sell hitting the density = absorption event
                    active_d["absorbed_sell_volume"] += q
                    active_d["sell_wave_volume"] += q
                    active_d["sell_wave_active"] = True
                    active_d["sell_wave_last_ts"] = current_time
                else:
                    # Buy bouncing off density = confirmation
                    active_d["bounce_buy_volume"] += q
                    if active_d["sell_wave_volume"] > 0:
                        active_d["bounce_confirmed"] = True

            # ── v13: End sell wave if timeout ──
            if active_d.get("sell_wave_active") and active_d.get("sell_wave_last_ts"):
                wave_age = current_time - active_d["sell_wave_last_ts"]
                if wave_age > ABSORPTION_WAVE_TIMEOUT:
                    if active_d["sell_wave_volume"] > 0:
                        ratio = active_d["sell_wave_volume"] / max(active_d["initial_volume"], 1.0)
                        if ratio >= 0.3:
                            print(f"[Absorption] {symbol} @ {density_price}: sell wave ended, "
                                  f"absorbed {active_d['sell_wave_volume']:.2f} ({ratio:.1%} of wall), "
                                  f"bounce={'YES' if active_d.get('bounce_confirmed') else 'NO'}",
                                  flush=True)
                    active_d["sell_wave_volume"] = 0.0
                    active_d["sell_wave_active"] = False

            # ── v13: Price approaching density (within 0.05%) ──
            proximity_pct = abs(p - density_price) / density_price * 100
            if proximity_pct <= 0.05 and not active_d.get("sell_wave_active"):
                # Price is very close to density - mark potential absorption start
                if is_buyer_maker:
                    active_d["sell_wave_volume"] += q
                    active_d["sell_wave_active"] = True
                    active_d["sell_wave_last_ts"] = current_time
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

        # Get absorption stats from active density tracking
        active_d = active_densities.get(symbol)
        if active_d and active_d.get("price") == bid_price:
            absorption_ratio = active_d["absorbed_sell_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0
            refill_count = active_d["refill_count"]
            lifetime_sec = current_time - active_d["first_seen_ts"]
        else:
            absorption_ratio = 0.0
            refill_count = 0
            lifetime_sec = 0.0

        opportunity = _score_bid_candidate_v13(
            num, bid_price, bid_val_usd, dynamic_min_volume, metrics,
            buy_vol_5s, sell_vol_5s, delta_ratio,
            absorption_ratio, refill_count, lifetime_sec
        )
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
                    "bounce_buy_volume": hist.get("bounce_buy_volume", 0.0),
                    "bounce_confirmed": hist.get("bounce_confirmed", False),
                    "sell_wave_volume": 0.0,
                    "sell_wave_active": False,
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
                    "bounce_buy_volume": 0.0,
                    "bounce_confirmed": False,
                    "sell_wave_volume": 0.0,
                    "sell_wave_active": False,
                    "last_seen_ts": current_time,
                }
            history_densities.pop(symbol, None)

        active_d = active_densities[symbol]
        lifetime_sec = current_time - active_d["first_seen_ts"]
        refill_count = active_d["refill_count"]
        refill_ratio = active_d["refill_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0
        absorption_ratio = active_d["absorbed_sell_volume"] / active_d["initial_volume"] if active_d["initial_volume"] > 0 else 0.0
        bounce_buy_volume = active_d.get("bounce_buy_volume", 0.0)
        bounce_confirmed = active_d.get("bounce_confirmed", False)

        if symbol in last_density and last_density[symbol] is not None and last_density[symbol][1] == bid_price_str:
            last_price_float, last_price_str, peak_vol = last_density[symbol]
            last_density[symbol] = (last_price_float, last_price_str, max(peak_vol, bid_volume))
            if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                last_density_emit[symbol] = current_time
                _emit_density(
                    symbol, bid_price, bid_volume, ask_vol, best["num"], data.get("E"), metrics,
                    lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                    buy_vol_5s, sell_vol_5s, best["opportunity"],
                    bounce_buy_volume, bounce_confirmed
                )
            return

        print(
            f"[v13] Density {symbol} @ {bid_price_str.rstrip('0')}, "
            f"${round(best['value_usd'], 2)}, score={best['opportunity']['opportunity_score']:.1f}, "
            f"abs={absorption_ratio:.2f}, refills={refill_count}, "
            f"lifetime={lifetime_sec:.0f}s, bounce={'YES' if bounce_confirmed else 'NO'}",
            flush=True
        )
        last_density[symbol] = (bid_price, bid_price_str, bid_volume)
        last_density_emit[symbol] = current_time
        _emit_density(
            symbol, bid_price, bid_volume, ask_vol, best["num"], data.get("E"), metrics,
            lifetime_sec, refill_count, refill_ratio, absorption_ratio,
            buy_vol_5s, sell_vol_5s, best["opportunity"],
            bounce_buy_volume, bounce_confirmed
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
                    bounce_buy_volume = active_d.get("bounce_buy_volume", 0.0)
                    bounce_confirmed = active_d.get("bounce_confirmed", False)
                else:
                    lifetime_sec = 0.0
                    refill_count = 0
                    refill_ratio = 0.0
                    absorption_ratio = 0.0
                    bounce_buy_volume = 0.0
                    bounce_confirmed = False

                opportunity = {
                    "opportunity_score": 0.0,
                    "distance_pct": max((metrics["best_bid"] - last_price_float) / metrics["best_bid"] * 100.0, 0.0),
                    "absorption_ratio": absorption_ratio,
                    "refill_count": refill_count,
                    "lifetime_sec": lifetime_sec,
                }
                if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                    last_density_emit[symbol] = current_time
                    _emit_density(
                        symbol, last_price_float, bid_volume, ask_vol, old_num, data.get("E"), metrics,
                        lifetime_sec, refill_count, refill_ratio, absorption_ratio,
                        buy_vol_5s, sell_vol_5s, opportunity,
                        bounce_buy_volume, bounce_confirmed
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
        {"opportunity_score": 0.0, "distance_pct": 0.0, "absorption_ratio": 0.0, "refill_count": 0, "lifetime_sec": 0.0},
        0.0, False
    )
