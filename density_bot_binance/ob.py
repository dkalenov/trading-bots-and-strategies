import binance
import asyncio
import time

# Словарь для отслеживания последней зафиксированной плотности по монетам
last_density = {}  # symbol -> last_seen_bid_price
last_density_emit = {}
rolling_depth = {}  # symbol -> { 'bid': float, 'ask': float }
DENSITY_HEARTBEAT_SECONDS = 0.5

msg_count = 0
last_log_time = 0.0

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
    msg_count += 1
    current_time = time.time()
    if last_log_time == 0.0:
        last_log_time = current_time
    elif current_time - last_log_time >= 60.0:
        proc_id = symbols[0].symbol if symbols else "Unknown"
        print(f"[WebSocket {proc_id}] Обработано {msg_count} обновлений стакана за 60 сек (в среднем {round(msg_count / 60.0, 1)}/сек)", flush=True)
        msg_count = 0
        last_log_time = current_time

    # если сообщение содержит данные
    if 'data' in msg:
        # получаем символ из названия стрима
        symbol = msg['stream'].split('@')[0].upper()
        
        data = msg['data']
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
                    
                    # Если эта плотность на той же цене, что и ранее, просто обновляем объем в памяти (и пиковый объем)
                    # и не отправляем дублирующий сигнал в основную очередь
                    if symbol in last_density and last_density[symbol] is not None and last_density[symbol][1] == bid_price_str:
                        last_price_float, last_price_str, peak_vol = last_density[symbol]
                        last_density[symbol] = (last_price_float, last_price_str, max(peak_vol, bid_volume))
                        if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                            last_density_emit[symbol] = current_time
                            main_queue.put((symbol, bid_price, bid_volume, ask_vol, num, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0])))
                        break
                    
                    # Если плотность обнаружена
                    print(f"Обнаружена плотность по паре {symbol} цена {bid_price_str.rstrip('0')}, объем "
                          f"${round(bid_val_usd, 2)} (ср. уровень bids: ${round(rolling_depth[symbol]['bid'], 2)}), который в "
                          f"{round(bid_volume / avg_ask_vol, 2)} раз больше среднего объема {len(asks)} asks", flush=True)
                    
                    last_density[symbol] = (bid_price, bid_price_str, bid_volume)
                    last_density_emit[symbol] = current_time
                    # добавляем информацию в очередь с таймстемпами для замера задержек
                    main_queue.put((symbol, bid_price, bid_volume, ask_vol, num, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0])))
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
                            if current_time - last_density_emit.get(symbol, 0.0) >= DENSITY_HEARTBEAT_SECONDS:
                                last_density_emit[symbol] = current_time
                                main_queue.put((symbol, last_price_float, bid_volume, ask_vol, old_num, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0])))
                        break
                
                if not still_exists:
                    print(f"Плотность по паре {symbol} на цене {last_price_str.rstrip('0')} исчезла или сильно уменьшилась! Отправляем мгновенный сигнал отмены.", flush=True)
                    last_density[symbol] = None
                    last_density_emit.pop(symbol, None)
                    main_queue.put((symbol, None, 0.0, 0.0, -1, time.perf_counter_ns(), data.get('E'), float(bids[0][0]), float(asks[0][0])))




