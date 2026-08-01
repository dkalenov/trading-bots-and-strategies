"""
Bollinger Bands Strategy — Live Testnet Adapter (Bybit Futures)

WebSocket for real-time price + candle close detection.
REST API for order execution.
"""

import math
import os
import sys
import time
import hmac
import hashlib
import json
import threading
import requests
from decimal import Decimal
from datetime import datetime

import websocket
import pandas as pd
import numpy as np

from strategy import (
    BollingerBandsCore, BollingerRSIFilter, BollingerSqueezeFilter,
    BollingerMeanReversion, Signal, StrategyParams,
    calculate_bollinger_bands, calculate_rsi, calculate_atr
)


# ── Config ────────────────────────────────────────────────────────────

TESTNET_BASE = "https://api-testnet.bybit.com"
MAINNET_BASE = "https://api.bybit.com"
WS_PUBLIC_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"
WS_PUBLIC_MAINNET = "wss://stream.bybit.com/v5/public/linear"
WS_PRIVATE = "wss://stream-testnet.bybit.com/v5/private"

API_KEY = os.environ.get("BYBIT_TESTNET_API_KEY", "")
API_SECRET = os.environ.get("BYBIT_TESTNET_API_SECRET", "")

_filters_cache = {}


# ── Exchange filters ──────────────────────────────────────────────────

def get_symbol_filters(symbol: str, base_url: str = TESTNET_BASE) -> dict:
    if symbol in _filters_cache:
        return _filters_cache[symbol]
    try:
        resp = requests.get(f"{base_url}/v5/market/instruments-info",
                            params={'category': 'linear', 'symbol': symbol}, timeout=10)
        data = resp.json()
        if data.get('retCode') == 0:
            instruments = data.get('result', {}).get('list', [])
            if instruments:
                inst = instruments[0]
                lot_filter = inst.get('lotSizeFilter', {})
                price_filter = inst.get('priceFilter', {})
                step = lot_filter.get('qtyStep', '0.001')
                min_qty = lot_filter.get('minOrderQty', '0.001')
                max_qty = lot_filter.get('maxOrderQty', '1000')
                tick = price_filter.get('tickSize', '0.01')
                min_notional = float(lot_filter.get('minOrderValue', '5'))
                filters = {
                    'step_size': _decimal_places(float(step)),
                    'min_qty': float(min_qty),
                    'max_qty': float(max_qty),
                    'tick_size': _decimal_places(float(tick)),
                    'min_notional': min_notional,
                }
                _filters_cache[symbol] = filters
                return filters
    except Exception as e:
        print(f"  Warning: Could not fetch filters: {e}")
    return {'step_size': 3, 'tick_size': 2, 'min_qty': 0.001, 'max_qty': 1000, 'min_notional': 5}


def _decimal_places(value: float) -> int:
    if value > 0:
        return -int(math.log10(value))
    return 0


def floor_to_step(qty: Decimal, step_size: int) -> Decimal:
    factor = 10 ** step_size
    return Decimal((qty * factor).__floor__()) / factor


def fmt_qty(qty: float, precision: int) -> str:
    return f"{qty:.{precision}f}"


def fmt_price(price: float, precision: int) -> str:
    return f"{price:.{precision}f}"


# ── Bybit API helpers ─────────────────────────────────────────────────

def _bybit_sign(params: dict, secret: str, timestamp: int) -> str:
    param_str = f"{timestamp}{API_KEY}5000" + json.dumps(params, separators=(',', ':'))
    return hmac.new(secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()


def _bybit_request(method: str, endpoint: str, params: dict, base_url: str = TESTNET_BASE) -> dict:
    timestamp = int(time.time() * 1000)
    headers = {
        'X-BAPI-API-KEY': API_KEY,
        'X-BAPI-TIMESTAMP': str(timestamp),
        'X-BAPI-RECV-WINDOW': '5000',
        'Content-Type': 'application/json',
    }

    if method == 'GET':
        query = '&'.join(f'{k}={v}' for k, v in sorted(params.items()))
        param_str = f"{timestamp}{API_KEY}5000" + query
        sign = hmac.new(API_SECRET.encode(), param_str.encode(), hashlib.sha256).hexdigest()
        headers['X-BAPI-SIGN'] = sign
        resp = requests.get(f"{base_url}{endpoint}", params=params, headers=headers, timeout=10)
    else:
        body = json.dumps(params)
        param_str = f"{timestamp}{API_KEY}5000" + body
        sign = hmac.new(API_SECRET.encode(), param_str.encode(), hashlib.sha256).hexdigest()
        headers['X-BAPI-SIGN'] = sign
        resp = requests.post(f"{base_url}{endpoint}", data=body, headers=headers, timeout=10)

    result = resp.json()
    if result.get('retCode') != 0:
        raise Exception(f"Bybit error: {result.get('retMsg', 'unknown')} (code={result.get('retCode')})")
    return result.get('result', {})


# ── Account ───────────────────────────────────────────────────────────

def get_account_balance(base_url: str = TESTNET_BASE) -> float:
    try:
        result = _bybit_request('GET', '/v5/account/wallet-balance', {'accountType': 'UNIFIED'}, base_url)
        coins = result.get('list', [{}])[0].get('coin', [])
        for c in coins:
            if c.get('coin') == 'USDT':
                return float(c.get('equity', 0))
    except Exception:
        pass
    return 0.0


def get_position(symbol: str, base_url: str = TESTNET_BASE) -> dict:
    try:
        result = _bybit_request('GET', '/v5/position/list',
                                {'category': 'linear', 'symbol': symbol}, base_url)
        positions = result.get('list', [])
        for p in positions:
            if p.get('symbol') == symbol:
                size = float(p.get('size', 0))
                side = p.get('side', '')
                return {
                    'side': side,
                    'size': size,
                    'entry_price': float(p.get('avgPrice', 0)),
                    'unrealized_pnl': float(p.get('unrealisedPnl', 0)),
                    'leverage': int(p.get('leverage', 1)),
                }
    except Exception:
        pass
    return {'side': '', 'size': 0, 'entry_price': 0, 'unrealized_pnl': 0, 'leverage': 1}


def set_leverage(symbol: str, leverage: int, base_url: str = TESTNET_BASE):
    _bybit_request('POST', '/v5/position/set-leverage', {
        'category': 'linear', 'symbol': symbol,
        'buyLeverage': str(leverage), 'sellLeverage': str(leverage),
    }, base_url)


# ── Klines ────────────────────────────────────────────────────────────

BYBIT_INTERVAL_MAP = {
    '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
    '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
    '1d': 'D', '1w': 'W', '1M': 'M',
}


def fetch_klines(symbol: str, interval: str, limit: int = 200,
                 base_url: str = TESTNET_BASE) -> pd.DataFrame:
    bybit_interval = BYBIT_INTERVAL_MAP.get(interval, interval)
    result = _bybit_request('GET', '/v5/market/kline', {
        'category': 'linear', 'symbol': symbol,
        'interval': bybit_interval, 'limit': str(limit),
    }, base_url)

    klines = result.get('list', [])
    if not klines:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    # Bybit returns newest-first
    klines.reverse()

    df = pd.DataFrame(klines, columns=[
        'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover'
    ])
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[col] = df[col].astype(float)
    df['Date'] = pd.to_datetime(df['Timestamp'].astype(int), unit='ms', utc=True)
    df.set_index('Date', inplace=True)
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]


# ── Order execution ───────────────────────────────────────────────────

def cancel_all_orders(symbol: str, base_url: str = TESTNET_BASE):
    try:
        _bybit_request('POST', '/v5/order/cancel-all', {
            'category': 'linear', 'symbol': symbol,
        }, base_url)
    except Exception:
        pass


def cancel_conditional_orders(symbol: str, base_url: str = TESTNET_BASE):
    try:
        _bybit_request('POST', '/v5/position/trading-stop', {
            'category': 'linear', 'symbol': symbol,
            'takeProfit': '', 'stopLoss': '', 'trailingStop': '',
        }, base_url)
    except Exception:
        pass


def place_market_order(symbol: str, side: str, quantity: float,
                       step_size: int, base_url: str = TESTNET_BASE) -> dict:
    return _bybit_request('POST', '/v5/order/create', {
        'category': 'linear',
        'symbol': symbol,
        'side': side,
        'orderType': 'Market',
        'qty': fmt_qty(quantity, step_size),
    }, base_url)


def place_stop_loss(symbol: str, side: str, stop_price: float,
                    quantity: float, tick_size: int, step_size: int,
                    base_url: str = TESTNET_BASE) -> dict:
    """Place SL as conditional order (Bybit algo)."""
    return _bybit_request('POST', '/v5/order/create', {
        'category': 'linear',
        'symbol': symbol,
        'side': side,
        'orderType': 'Market',
        'qty': fmt_qty(quantity, step_size),
        'triggerDirection': 1 if side == 'Sell' else 2,
        'triggerPrice': fmt_price(stop_price, tick_size),
        'triggerBy': 'LastPrice',
        'reduceOnly': True,
    }, base_url)


def place_take_profit(symbol: str, side: str, tp_price: float,
                      quantity: float, tick_size: int, step_size: int,
                      base_url: str = TESTNET_BASE) -> dict:
    """Place TP as conditional order (Bybit algo)."""
    return _bybit_request('POST', '/v5/order/create', {
        'category': 'linear',
        'symbol': symbol,
        'side': side,
        'orderType': 'Market',
        'qty': fmt_qty(quantity, step_size),
        'triggerDirection': 2 if side == 'Sell' else 1,
        'triggerPrice': fmt_price(tp_price, tick_size),
        'triggerBy': 'LastPrice',
        'reduceOnly': True,
    }, base_url)


# ── Position sizing ───────────────────────────────────────────────────

def compute_quantity(equity: float, risk_pct: float, bb_width: float,
                     entry_price: float, leverage: int, filters: dict) -> float:
    step_size = filters.get('step_size', 3)
    min_qty = filters.get('min_qty', 0.001)
    max_qty = filters.get('max_qty', 1000)
    min_notional = filters.get('min_notional', 5) * 1.15

    risk_amount = equity * risk_pct
    stop_distance = bb_width * 0.5  # SL at half BB width
    if stop_distance <= 0:
        return 0

    raw_qty = Decimal(str(risk_amount)) / Decimal(str(stop_distance))
    qty = floor_to_step(raw_qty, step_size)

    if qty < Decimal(str(min_qty)):
        qty = Decimal(str(min_qty))

    max_notional = equity * 0.15
    if entry_price > 0:
        max_qty_from_notional = floor_to_step(
            Decimal(str(max_notional)) / Decimal(str(entry_price)), step_size
        )
        if qty > max_qty_from_notional:
            qty = max_qty_from_notional

    if float(qty) > max_qty:
        qty = floor_to_step(Decimal(str(max_qty)), step_size)

    if entry_price > 0:
        notional = float(qty) * entry_price
        if notional < min_notional:
            needed = Decimal(str(min_notional)) / Decimal(str(entry_price))
            qty = floor_to_step(needed, step_size)

    return float(qty)


# ── Market data WebSocket ─────────────────────────────────────────────

class MarketDataWS:
    """Bybit public WebSocket: kline stream for candle close detection + tickers for price."""

    def __init__(self, symbol: str, interval: str, on_tick_cb, on_candle_close_cb,
                 ws_url: str = WS_PUBLIC_TESTNET):
        self.symbol = symbol
        self.interval = interval
        self.on_tick = on_tick_cb
        self.on_candle_close = on_candle_close_cb
        self.ws_url = ws_url
        self._ws = None
        self._running = False
        self._thread = None
        self._last_close_time = 0

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def _run(self):
        backoff = 5
        while self._running:
            try:
                bybit_interval = BYBIT_INTERVAL_MAP.get(self.interval, self.interval)
                topic = f"kline.{bybit_interval}.{self.symbol}"

                def on_open(ws):
                    print(f"  WS connected: {topic}")
                    sub_msg = json.dumps({"op": "subscribe", "args": [topic]})
                    ws.send(sub_msg)

                def on_message(ws, message):
                    try:
                        msg = json.loads(message)
                        if 'topic' not in msg:
                            return
                        data = msg.get('data', [])
                        if not data:
                            return
                        k = data[0]
                        close = float(k.get('close', 0))
                        if close > 0:
                            self.on_tick(close)

                        # Detect candle close
                        if k.get('confirm', False):
                            ts = k.get('start', 0)
                            if ts != self._last_close_time:
                                self._last_close_time = ts
                                self.on_candle_close()
                    except Exception as e:
                        print(f"  WS message error: {e}")

                def on_error(ws, error):
                    print(f"  WS error: {str(error)[:200]}")

                def on_close(ws, status, msg):
                    print(f"  WS closed")

                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
                backoff = 5
            except Exception as e:
                print(f"  WS error: {e}")

            if self._running:
                print(f"  WS reconnecting in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)


# ── Main loop ─────────────────────────────────────────────────────────

def run_live(symbol='BTCUSDT', interval='1h', variant='basic',
             bb_timeperiod=20, bb_nbdevup=2.0, bb_nbdevdn=2.0,
             take_profit_mult=3.0, stop_loss_mult=1.5,
             leverage=10, risk_pct=0.01,
             dry_run=True, debug=False, testnet=True, mainnet_data=False):

    params = StrategyParams(
        bb_timeperiod=bb_timeperiod,
        bb_nbdevup=bb_nbdevup,
        bb_nbdevdn=bb_nbdevdn,
        take_profit_multiplier=take_profit_mult,
        stop_loss_multiplier=stop_loss_mult,
    )

    base_url = TESTNET_BASE if testnet else MAINNET_BASE
    data_url = MAINNET_BASE if mainnet_data else base_url
    ws_url = WS_PUBLIC_MAINNET if mainnet_data else (WS_PUBLIC_TESTNET if testnet else WS_PUBLIC_MAINNET)

    print(f"\n{'='*70}")
    print(f"  Bollinger Bands Live — {symbol} {interval}")
    print(f"{'='*70}")
    print(f"  Mode:       {'DRY RUN' if dry_run else 'LIVE'} ({'testnet' if testnet else 'mainnet'})")
    if mainnet_data:
        print(f"  Data:       mainnet (real prices)")
    print(f"  Variant:    {variant}")
    print(f"  BB:         ({bb_timeperiod}, {bb_nbdevup}, {bb_nbdevdn})")
    print(f"  TP/SL:      {take_profit_mult}x / {stop_loss_mult}x BB width")
    print(f"  Leverage:   {leverage}x")
    print(f"  Risk/Trade: {risk_pct*100}%")
    if debug:
        print(f"  DEBUG:      ON (force open on every candle)")
    print(f"{'='*70}")

    if not API_KEY:
        print("\n  ERROR: Set BYBIT_TESTNET_API_KEY and BYBIT_TESTNET_API_SECRET env vars")
        sys.exit(1)

    if debug and not dry_run:
        print("\n  ERROR: --debug cannot be combined with --live.")
        print("  --debug ignores the strategy signal and force-opens alternating")
        print("  positions every candle purely to test order placement/connectivity.")
        print("  It must never be used to place real orders. Run --debug alone")
        print("  (dry-run) to test connectivity, or --live alone to trade the")
        print("  actual strategy signal.")
        sys.exit(1)

    filters = get_symbol_filters(symbol, base_url)
    print(f"  Filters: step={10**-filters['step_size']}, tick={10**-filters['tick_size']}, "
          f"min_qty={filters['min_qty']}, min_notional={filters['min_notional']}")

    if not dry_run:
        try:
            set_leverage(symbol, leverage, base_url)
            print(f"  Leverage set to {leverage}x")
        except Exception as e:
            print(f"  Warning: Could not set leverage: {e}")

    # Initialize strategy
    if variant == 'rsi_filter':
        strategy = BollingerRSIFilter(params)
    elif variant == 'squeeze':
        strategy = BollingerSqueezeFilter(params)
    elif variant == 'mean_reversion':
        strategy = BollingerMeanReversion(params)
    else:
        strategy = BollingerBandsCore(params)

    # Warm up with historical klines
    warmup_limit = max(bb_timeperiod + 50, 100)
    df = fetch_klines(symbol, interval, warmup_limit, data_url)
    if len(df) < bb_timeperiod + 10:
        print(f"  ERROR: Not enough data ({len(df)} bars)")
        sys.exit(1)

    closes = df['Close'].values
    upper, middle, lower = calculate_bollinger_bands(closes, bb_timeperiod, bb_nbdevup, bb_nbdevdn)
    rsi = calculate_rsi(closes, params.rsi_period)

    # Feed historical data to strategy
    for i in range(bb_timeperiod + 1, len(closes)):
        if variant == 'rsi_filter':
            strategy.update(closes[i], upper[i], middle[i], lower[i], rsi[i])
        elif variant in ('squeeze',):
            bb_widths = (upper - lower) / np.where(middle != 0, middle, 1)
            strategy.update(closes[i], upper[i], middle[i], lower[i], bb_widths, i)
        else:
            strategy.update(closes[i], upper[i], middle[i], lower[i])

    last_close = closes[-1]
    last_upper = upper[-1]
    last_lower = lower[-1]
    last_middle = middle[-1]
    last_bb_width = abs(last_upper - last_lower)
    print(f"  Warmup: {len(closes)} bars, close={last_close:.2f}, "
          f"BB=({last_lower:.2f}/{last_middle:.2f}/{last_upper:.2f}), width={last_bb_width:.2f}")

    # State
    last_signal_time = [0]
    debug_side = [1]

    def on_tick(close):
        """Display-only. We deliberately do NOT evaluate the strategy here.

        The backtester generates at most one signal per closed candle (using
        that candle's final close). If we called strategy.update() on every
        incoming tick, we'd mutate the strategy's internal crossover state
        many times before the candle actually closes, so live signals would
        react to intra-candle noise the backtest never saw — a real behaviour
        mismatch between what was tested and what would trade live. All
        decisions are made once per candle in on_candle_close() instead.
        """
        now = time.time()
        if now - last_signal_time[0] < 30:
            return
        last_signal_time[0] = now
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"  [{ts}] price={close:.2f}", end='\r')

    def on_candle_close():
        """Recalculate indicators on the newly closed candle and evaluate
        exactly one signal from it — this mirrors the backtester's bar-by-bar
        loop (signal computed from bar i's close, using bands/RSI that include
        bar i). Debug mode short-circuits into a forced alternating trade for
        connectivity testing only, and never runs the real strategy signal."""
        nonlocal last_upper, last_lower, last_middle, last_bb_width, last_close

        try:
            df_new = fetch_klines(symbol, interval, bb_timeperiod + 50, data_url)
        except Exception as e:
            print(f"  [WARN] fetch_klines failed: {e}")
            return
        if len(df_new) < bb_timeperiod + max(params.rsi_period, 1) + 1:
            return

        closes_new = df_new['Close'].values
        u, m, l = calculate_bollinger_bands(closes_new, bb_timeperiod, bb_nbdevup, bb_nbdevdn)
        rsi_new = calculate_rsi(closes_new, params.rsi_period)
        last_close = closes_new[-1]
        last_upper, last_middle, last_lower = u[-1], m[-1], l[-1]
        last_bb_width = abs(last_upper - last_lower)

        if not debug:
            if last_bb_width <= 0:
                return

            if variant == 'rsi_filter':
                signal = strategy.update(last_close, last_upper, last_middle, last_lower, rsi_new[-1])
            elif variant == 'squeeze':
                bb_widths_full = (u - l) / np.where(m != 0, m, 1)
                signal = strategy.update(last_close, last_upper, last_middle, last_lower,
                                          bb_widths_full, len(bb_widths_full) - 1)
            else:
                signal = strategy.update(last_close, last_upper, last_middle, last_lower)

            if signal == Signal.HOLD:
                return

            pos = get_position(symbol, base_url)
            has_position = pos['size'] != 0
            ts = datetime.now().strftime('%H:%M:%S')

            if signal in (Signal.BUY, Signal.SELL) and not has_position:
                direction = 1 if signal == Signal.BUY else -1
                side_entry = 'Buy' if direction == 1 else 'Sell'
                side_close = 'Sell' if direction == 1 else 'Buy'

                entry_price = last_close
                bb_width = last_bb_width
                sl = entry_price - direction * bb_width * stop_loss_mult * 0.5
                tp = entry_price + direction * bb_width * take_profit_mult * 0.5

                balance = get_account_balance(base_url) if not dry_run else 100000
                quantity = compute_quantity(balance, risk_pct, bb_width, entry_price, leverage, filters)

                if quantity <= 0:
                    print(f"  [{ts}] {side_entry} SIGNAL rejected: qty=0")
                    return

                notional = quantity * entry_price
                print(f"\n  [{ts}] {side_entry} SIGNAL")
                print(f"    Entry:     {entry_price:.2f}")
                print(f"    SL:        {sl:.2f} ({abs(entry_price-sl)/entry_price*100:.2f}%)")
                print(f"    TP:        {tp:.2f} ({abs(tp-entry_price)/entry_price*100:.2f}%)")
                print(f"    Quantity:  {fmt_qty(quantity, filters['step_size'])}")
                print(f"    Notional:  ${notional:.2f}")
                print(f"    BB Width:  {bb_width:.2f}")

                if not dry_run:
                    try:
                        cancel_all_orders(symbol, base_url)
                    except Exception as e:
                        print(f"    Cancel error: {e}")

                    try:
                        order = place_market_order(symbol, side_entry, quantity,
                                                   filters['step_size'], base_url)
                        fill_price = float(order.get('avgPrice', entry_price))
                        if fill_price > 0:
                            entry_price = fill_price
                            sl = entry_price - direction * bb_width * stop_loss_mult * 0.5
                            tp = entry_price + direction * bb_width * take_profit_mult * 0.5
                            print(f"    Fill:      {fill_price:.2f}")
                    except Exception as e:
                        print(f"    Entry ERROR: {e}")
                        return

                    try:
                        place_stop_loss(symbol, side_close, sl, quantity,
                                        filters['tick_size'], filters['step_size'], base_url)
                        print(f"    SL placed: {sl:.2f}")
                    except Exception as e:
                        print(f"    SL ERROR: {e}")

                    try:
                        place_take_profit(symbol, side_close, tp, quantity,
                                          filters['tick_size'], filters['step_size'], base_url)
                        print(f"    TP placed: {tp:.2f}")
                    except Exception as e:
                        print(f"    TP ERROR: {e}")

            return

        # Debug mode: force open on every candle (connectivity test only —
        # ignores the strategy signal entirely; --live is blocked above, so
        # this can only ever run against dry_run or testnet).
        pos = get_position(symbol, base_url)
        has_position = pos['size'] != 0
        close = last_close

        if has_position:
            side_close = 'Sell' if pos['side'] == 'Buy' else 'Buy'
            try:
                cancel_all_orders(symbol, base_url)
                place_market_order(symbol, side_close, abs(pos['size']),
                                   filters['step_size'], base_url)
                print(f"  [DEBUG] Closed position")
            except Exception as e:
                print(f"  [DEBUG] Close ERROR: {e}")
            time.sleep(1)

        direction = debug_side[0]
        debug_side[0] = -debug_side[0]

        side_entry = 'Buy' if direction == 1 else 'Sell'
        side_close = 'Sell' if direction == 1 else 'Buy'
        entry_price = close
        bb_width = last_bb_width
        sl = entry_price - direction * bb_width * stop_loss_mult * 0.5
        tp = entry_price + direction * bb_width * take_profit_mult * 0.5

        balance = get_account_balance(base_url) if not dry_run else 100000
        quantity = compute_quantity(balance, risk_pct, bb_width, entry_price, leverage, filters)
        if quantity <= 0:
            print(f"  [DEBUG] qty=0, skipping")
            return

        ts = datetime.now().strftime('%H:%M:%S')
        print(f"  [{ts}] [DEBUG] {side_entry} @ {entry_price:.2f}")

        if not dry_run:
            try:
                order = place_market_order(symbol, side_entry, quantity,
                                           filters['step_size'], base_url)
                fill_price = float(order.get('avgPrice', entry_price))
                if fill_price > 0:
                    entry_price = fill_price
                    sl = entry_price - direction * bb_width * stop_loss_mult * 0.5
                    tp = entry_price + direction * bb_width * take_profit_mult * 0.5
                    print(f"    Fill: {fill_price:.2f}")
            except Exception as e:
                print(f"    Entry ERROR: {e}")
                return
            try:
                place_stop_loss(symbol, side_close, sl, quantity,
                                filters['tick_size'], filters['step_size'], base_url)
                place_take_profit(symbol, side_close, tp, quantity,
                                  filters['tick_size'], filters['step_size'], base_url)
                print(f"    SL={sl:.2f} TP={tp:.2f}")
            except Exception as e:
                print(f"    SL/TP ERROR: {e}")

    # Start WebSocket
    market_ws = MarketDataWS(symbol, interval, on_tick, on_candle_close, ws_url)
    market_ws.start()

    print(f"\n  Waiting for signals... (Ctrl+C to stop)\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Stopping...")
        market_ws.stop()
        print("  Stopped.")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Bollinger Bands Live Testnet (Bybit)')
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--interval', default='1h')
    parser.add_argument('--variant', default='basic',
                        choices=['basic', 'rsi_filter', 'squeeze', 'mean_reversion'])
    parser.add_argument('--bb-timeperiod', type=int, default=20)
    parser.add_argument('--bb-nbdevup', type=float, default=2.0)
    parser.add_argument('--bb-nbdevdn', type=float, default=2.0)
    parser.add_argument('--tp-multiplier', type=float, default=3.0)
    parser.add_argument('--sl-multiplier', type=float, default=1.5)
    parser.add_argument('--leverage', type=int, default=10)
    parser.add_argument('--risk-pct', type=float, default=0.01)
    parser.add_argument('--live', action='store_true', help='Enable real orders')
    parser.add_argument('--mainnet', action='store_true', help='Use mainnet (default: testnet)')
    parser.add_argument('--mainnet-data', action='store_true', help='Use mainnet for price data (testnet has fake data for some symbols)')
    parser.add_argument('--debug', action='store_true', help='Force open on every candle')

    args = parser.parse_args()

    run_live(
        symbol=args.symbol,
        interval=args.interval,
        variant=args.variant,
        bb_timeperiod=args.bb_timeperiod,
        bb_nbdevup=args.bb_nbdevup,
        bb_nbdevdn=args.bb_nbdevdn,
        take_profit_mult=args.tp_multiplier,
        stop_loss_mult=args.sl_multiplier,
        leverage=args.leverage,
        risk_pct=args.risk_pct,
        dry_run=not args.live,
        testnet=not args.mainnet,
        mainnet_data=args.mainnet_data,
        debug=args.debug,
    )
