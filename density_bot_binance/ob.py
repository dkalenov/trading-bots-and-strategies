import binance
import asyncio
import time
from collections import deque

# Словарь для отслеживания последней зафиксированной плотности по монетам
last_density = {}  # symbol -> (last_seen_bid_price, last_seen_bid_price_str, last_seen_bid_volume)
last_density_emit = {}
rolling_depth = {}  # symbol -> { 'bid': float, 'ask': float }
DENSITY_HEARTBEAT_SECONDS = 0.5

msg_count = 0
last_log_time = 0.0

# v9 order flow tracking globals
trades_window = {}      # symbol -> deque of (t_time, price, qty, is_buyer_maker)
active_densities = {}   # symbol -> dict of active density stats
history_densities = {}  # symbol -> dict of inactive density stats (flicker protection)

# функция для запуска процесса вебсокета
def run(_symbols, _depth, _ask_volume, _main_queue, testnet):
    global symbols
    global depth
    global ask_volume
    global main_queue
    symbols = _symbols
    depth = _depth
    ask_volume = _ask_volume
    main_queue = _main_queue
    print(f"Запущен процесс вебсокета для следующих пар: {', '.join([sym.symbol for sym in symbols])}", flush=True)
    asyncio.run(main(testnet))

# основная функция вебсокета
async def main(testnet=False):
    global client
    global websocket
    # создаем клиент
    client = binance.Futures(asynced=True, testnet=testnet)
    # список для стримов
    streams = []
    for sym in symbols:
        # добавляем стримы в список с частотой обновления 100мс для снижения задержек
        streams.append(f"{sym.symbol.lower()}@depth{depth}@100ms")
        # добавляем стрим сделок для анализа ленты
        streams.append(f"{sym.symbol.lower()}@aggTrade")
    # подключаемся к вебсокету
    websocket = await client.websocket(streams, on_message=on_message, on_error=ws_error)
    while websocket.working:
        await asyncio.sleep(1)

# функция для обработки ошибок вебсокета
async def ws_error(ws, error):
    print(f"ws_error: {error}", flush=True)

# функция для обработки сообщений
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

    # если сообщение содержит данные
    if 'data' in msg:
        stream = msg.get('stream', '')
        data = msg['data']
        symbol = stream.split('@')[0].upper()

        # 1. ОБРАБОТКА СТРИМА СДЕЛАК (aggTrade)
        if 'aggtrade' in stream.lower():
            p = float(data['p'])
            q = float(data['q'])
            is_buyer_maker = bool(data['m']) # True = sell market order, False = buy market order
            t_time = float(data['T']) / 1000.0

            if symbol not in trades_window:
                trades_window[symbol] = deque()
            trades_window[symbol].append((t_time, p, q, is_buyer_maker))

            # Очищаем сделки старше 5 секунд
            while trades_window[symbol] and current_time - trades_window[symbol][0][0] > 5.0:
                trades_window[symbol].popleft()

            # Накапливаем поглощение рыночных продаж (absorbed_sell_volume) для активной плотности
            if symbol in active_densities and active_densities[symbol] is not None:
                active_d = active_densities[symbol]
                density_price = active_d['price']
                # Проверяем нахождение в диапазоне ±0.05% от цены плотности
                if density_price * 0.9995 <= p <= density_price * 1.0005:
                    if is_buyer_maker: # рыночная продажа бьет по бидам
                        active_d['absorbed_sell_volume'] += q
            return

        # 2. ОБРАБОТКА СТРИМА СТАКАНА (depth)
        elif 'depth' in stream.lower():
            bids = data.get('bids') or data.get('b')
            asks = data.get('asks') or data.get('a')
            
            if not bids or not asks:
                return
                
            # Считаем объем всех ask'ов и средний объем уровня
            ask_vol = sum([float(ask[1]) for ask in asks])
            if ask_vol <= 0 or len(asks) == 0:
                return
            avg_ask_vol = ask_vol / len(asks)

            # Считаем текущий средний объем уровня из топ-10 bids и asks в USD для скользящего EMA-профиля стакана
            top_bids = bids[:10]
            top_asks = asks[:10]
            current_avg_bid_val = sum([float(b[0]) * float(b[1]) for b in top_bids]) / len(top_bids) if top_bids else 0.0
            current_avg_ask_val = sum([float(a[0]) * float(a[1]) for a in top_asks]) / len(top_asks) if top_asks else 0.0

            if symbol not in rolling_depth:
                rolling_depth[symbol] = {'bid': current_avg_bid_val, 'ask': current_avg_ask_val}
            else:
                alpha = 0.01  # коэффициент сглаживания EMA (~100 обновлений стакана)
                rolling_depth[symbol]['bid'] = rolling_depth[symbol]['bid'] * (1 - alpha) + current_avg_bid_val * alpha
                rolling_depth[symbol]['ask'] = rolling_depth[symbol]['ask'] * (1 - alpha) + current_avg_ask_val * alpha

            found_density = False
            # ищем bid, объем которого больше или равен среднему объему асков в ask_volume раз
            for num, bid in enumerate(bids):
                bid_volume = float(bid[1])
                
                # Условие 1: bid больше среднего объема асков в ask_volume раз
                if bid_volume >= avg_ask_vol * ask_volume:
                    bid_price = float(bid[0])
                    bid_val_usd = bid_price * bid_volume
                    
                    # Условие 2: bid превышает исторический средний уровень стакана (EMA за 30м) в ask_volume раз ИЛИ больше жесткого пола в $20,000 USD
                    historical_min_volume = rolling_depth[symbol]['bid'] * ask_volume
                    dynamic_min_volume = max(historical_min_volume, 20000.0)
                    
                    if bid_val_usd >= dynamic_min_volume:
                        found_density = True
                        bid_price_str = bid[0]
                        
                        # --- ИНТЕГРАЦИЯ ORDER FLOW METRICS ---
                        # Проверяем, есть ли уже активная плотность на этой же цене
                        if symbol in active_densities and active_densities[symbol] is not None and active_densities[symbol]['price'] == bid_price:
                            active_d = active_densities[symbol]
                            prev_vol = active_d['last_volume']
                            
                            # Детекция refill (объем вырос на >20%)
                            if bid_volume > prev_vol * 1.20:
                                active_d['refill_count'] += 1
                                active_d['refill_volume'] += (bid_volume - prev_vol)
                                print(f"[Refill Detected] {symbol} @ {bid_price}: {prev_vol} -> {bid_volume} (+{round(bid_volume - prev_vol, 2)})", flush=True)
                            
                            active_d['last_volume'] = bid_volume
                            active_d['last_seen_ts'] = current_time
                        else:
                            # Плотность новая или изменилась цена.
                            # Проверяем flicker protection: была ли эта же плотность активна менее 2 сек назад?
                            hist = history_densities.get(symbol)
                            if hist and hist['price'] == bid_price and current_time - hist['inactive_ts'] <= 2.0:
                                # Восстанавливаем состояние
                                active_densities[symbol] = {
                                    'price': bid_price,
                                    'first_seen_ts': hist['first_seen_ts'],
                                    'refill_count': hist['refill_count'],
                                    'refill_volume': hist['refill_volume'],
                                    'initial_volume': hist['initial_volume'],
                                    'last_volume': bid_volume,
                                    'absorbed_sell_volume': hist['absorbed_sell_volume'],
                                    'last_seen_ts': current_time
                                }
                                print(f"[Flicker Recovery] {symbol} @ {bid_price} - restored lifetime clock ({round(current_time - hist['first_seen_ts'], 1)}s)", flush=True)
                            else:
                                # Инициализируем новую плотность
                                active_densities[symbol] = {
                                    'price': bid_price,
                                    'first_seen_ts': current_time,
                                    'refill_count': 0,
                                    'refill_volume': 0.0,
                                    'initial_volume': bid_volume,
                                    'last_volume': bid_volume,
                                    'absorbed_sell_volume': 0.0,
                                    'last_seen_ts': current_time
                                }
                            history_densities.pop(symbol, None)
                        
                        active_d = active_densities[symbol]
                        lifetime_sec = current_time - active_d['first_seen_ts']
                        refill_count = active_d['refill_count']
                        refill_ratio = active_d['refill_volume'] / active_d['initial_volume'] if active_d['initial_volume'] > 0 else 0.0
                        absorption_ratio = active_d['absorbed_sell_volume'] / active_d['initial_volume'] if active_d['initial_volume'] > 0 else 0.0
                        
                        # Рассчитываем Tape Delta
                        buy_vol_5s = 0.0
                        sell_vol_5s = 0.0
                        if symbol in trades_window:
                            for t in trades_window[symbol]:
                                if t[3]: # is_buyer_maker == True (market sell)
                                    sell_vol_5s += t[2]
                                else: # is_buyer_maker == False (market buy)
                                    buy_vol_5s += t[2]

                        # Если эта плотность на той же цене, что и ранее, просто обновляем объем в памяти (и пиковый объем)
                        if symbol in last_density and last_density[symbol] is not None and last_density[symbol][1] == bid_price_str:
                            last_price_float, last_price_str, peak_vol = last_density[symbol]
                            last_density[symbol] = (last_price_float, last_price_str, max(peak_vol, bid_volume))
                            if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                                last_density_emit[symbol] = current_time
                                main_queue.put((symbol, bid_price, bid_volume, ask_vol, num, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0]),
                                                lifetime_sec, refill_count, refill_ratio, absorption_ratio, buy_vol_5s, sell_vol_5s))
                            break
                        
                        # Если плотность обнаружена впервые
                        print(f"Обнаружена плотность по паре {symbol} цена {bid_price_str.rstrip('0')}, объем "
                              f"${round(bid_val_usd, 2)} (ср. уровень bids: ${round(rolling_depth[symbol]['bid'], 2)}), который в "
                              f"{round(bid_volume / avg_ask_vol, 2)} раз больше среднего объема {len(asks)} asks", flush=True)
                        
                        last_density[symbol] = (bid_price, bid_price_str, bid_volume)
                        last_density_emit[symbol] = current_time
                        main_queue.put((symbol, bid_price, bid_volume, ask_vol, num, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0]),
                                        lifetime_sec, refill_count, refill_ratio, absorption_ratio, buy_vol_5s, sell_vol_5s))
                        break
                    
            # Если плотность исчезла или уменьшилась, отправляем сигнал отмены в основную очередь
            if not found_density:
                if symbol in last_density and last_density[symbol] is not None:
                    last_price_float, last_price_str, peak_vol = last_density[symbol]
                    
                    # Ищем, осталась ли лимитка на том же уровне цены (используем быстрое сравнение строк)
                    still_exists = False
                    for old_num, bid in enumerate(bids):
                        if bid[0] == last_price_str:
                            bid_volume = float(bid[1])
                            # Сравниваем с пиковым объемом (peak_vol)
                            if bid_volume >= peak_vol * 0.5 and last_price_float * bid_volume >= 8000.0:
                                still_exists = True
                                # Обновляем пиковый объем, если текущий объем вырос
                                last_density[symbol] = (last_price_float, last_price_str, max(peak_vol, bid_volume))
                                
                                # --- ОБНОВЛЯЕМ ORDER FLOW ДЛЯ ТЕКУЩЕЙ ПЛОТНОСТИ ---
                                if symbol in active_densities and active_densities[symbol] is not None:
                                    active_d = active_densities[symbol]
                                    prev_vol = active_d['last_volume']
                                    if bid_volume > prev_vol * 1.20:
                                        active_d['refill_count'] += 1
                                        active_d['refill_volume'] += (bid_volume - prev_vol)
                                    active_d['last_volume'] = bid_volume
                                    active_d['last_seen_ts'] = current_time
                                    
                                    lifetime_sec = current_time - active_d['first_seen_ts']
                                    refill_count = active_d['refill_count']
                                    refill_ratio = active_d['refill_volume'] / active_d['initial_volume'] if active_d['initial_volume'] > 0 else 0.0
                                    absorption_ratio = active_d['absorbed_sell_volume'] / active_d['initial_volume'] if active_d['initial_volume'] > 0 else 0.0
                                    
                                    buy_vol_5s = 0.0
                                    sell_vol_5s = 0.0
                                    if symbol in trades_window:
                                        for t in trades_window[symbol]:
                                            if t[3]:
                                                sell_vol_5s += t[2]
                                            else:
                                                buy_vol_5s += t[2]
                                else:
                                    lifetime_sec = 0.0
                                    refill_count = 0
                                    refill_ratio = 0.0
                                    absorption_ratio = 0.0
                                    buy_vol_5s = 0.0
                                    sell_vol_5s = 0.0

                                if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                                    last_density_emit[symbol] = current_time
                                    main_queue.put((symbol, last_price_float, bid_volume, ask_vol, old_num, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0]),
                                                    lifetime_sec, refill_count, refill_ratio, absorption_ratio, buy_vol_5s, sell_vol_5s))
                            break
                    
                    if not still_exists:
                        print(f"Плотность по паре {symbol} на цене {last_price_str.rstrip('0')} исчезла или сильно уменьшилась! Отправляем мгновенный сигнал отмены.", flush=True)
                        
                        # Сохраняем во flicker protection перед удалением
                        if symbol in active_densities and active_densities[symbol] is not None:
                            active_d = active_densities[symbol]
                            history_densities[symbol] = {
                                'price': active_d['price'],
                                'first_seen_ts': active_d['first_seen_ts'],
                                'refill_count': active_d['refill_count'],
                                'refill_volume': active_d['refill_volume'],
                                'initial_volume': active_d['initial_volume'],
                                'absorbed_sell_volume': active_d['absorbed_sell_volume'],
                                'inactive_ts': current_time
                            }
                        active_densities[symbol] = None
                        
                        last_density[symbol] = None
                        last_density_emit.pop(symbol, None)
                        main_queue.put((symbol, None, 0.0, 0.0, -1, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0]),
                                        0.0, 0, 0.0, 0.0, 0.0, 0.0))
