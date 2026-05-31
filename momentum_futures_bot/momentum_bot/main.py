"""Momentum Trading Bot — Main Entry Point.
Structured v4.1 + Adaptive Sizing v4.4.
"""
from __future__ import annotations

import sys
import asyncio
import configparser
import logging
import traceback
import json
import os
import signal as signal_module
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# Add parent to path for momentum_core
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import binance
import db
import utils
import tg
from signal_engine import LiveSignalEngine
from risk_manager import RiskManager
from position_manager import PositionManager

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bot")

# Shutdown coordination
shutdown_event = asyncio.Event()

# Lock file
LOCK_FILE = Path(__file__).resolve().parent / "momentum_bot.lock"

# Globals
conf: db.ConfigInfo = None
client: binance.Futures = None
all_symbols: dict = {}
all_prices: dict = {}
positions: dict = {}
entries_this_bar: int = 0

# Queues for TG messages
message_queue = asyncio.Queue()
error_queue = asyncio.Queue()

# Anti-spam
error_send_cache = defaultdict(lambda: 0)
ERROR_SEND_INTERVAL = 300

# WebSocket
userdata_ws = None
market_ws   = None   # market-data stream (markPrice ticks for TP1 detection)
symbol_locks = defaultdict(asyncio.Lock)
tg_channel = None
error_channel = None

# In-memory trade cache for WS TP1 handler (avoids DB query on every price tick)
_active_trades: dict[str, object] = {}   # symbol → db.Trade
# In-progress TP1 guard (prevent double fire)
_tp1_in_progress: set[str] = set()

# Engine instances
signal_engine: LiveSignalEngine = None
risk_manager: RiskManager = None
position_manager: PositionManager = None

# Order type sets (used everywhere — reconciliation, WS, cache)
STOP_ORDER_TYPES = {"STOP_MARKET", "STOP"}
TAKE_ORDER_TYPES = {"LIMIT", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"}

# ============================================================
# In-memory protection state cache
# ============================================================
# Tracks whether the bot has placed (and not yet seen canceled) a stop
# for each open position.  Updated by:
#   - open_position / TP1 rollover  → "protected"
#   - WS ORDER_TRADE_UPDATE cancel  → "unprotected"
#   - position close                → deleted
# Reconciliation checks this cache FIRST; REST is only called
# as a safety net before any destructive action (force-close).
_protection_state: dict[str, str] = {}   # symbol → "protected" | "unprotected"


def _prot_set(symbol: str, state: str) -> None:
    """Set protection state for a symbol."""
    _protection_state[symbol] = state
    logger.debug(f"Protection[{symbol}] = {state}")


def _prot_clear(symbol: str) -> None:
    """Clear protection state (position closed)."""
    _protection_state.pop(symbol, None)


def _prot_is_protected(symbol: str) -> bool:
    """True if bot believes this symbol has stop protection."""
    return _protection_state.get(symbol) == "protected"


async def _reseed_protection_cache() -> None:
    """Re-fetch orders from REST and update protection cache.

    Called on WS reconnect to fill gaps from missed events.
    Only updates symbols with open positions; does NOT clear existing state
    for symbols it can't verify (to avoid false-negative on API failure).
    """
    try:
        open_syms = {s for s, v in positions.items() if v}
        if not open_syms:
            return

        for sym in open_syms:
            try:
                # Check standard orders
                std = await client.get_orders(symbol=sym)
                std = std if isinstance(std, list) else []
                if any(o.get("type") in STOP_ORDER_TYPES for o in std):
                    _prot_set(sym, "protected")
                    continue

                # Check algo orders
                algo_raw = await client.get_algo_orders(symbol=sym)
                algo = (
                    algo_raw.get("algoOrders", []) if isinstance(algo_raw, dict)
                    else (algo_raw if isinstance(algo_raw, list) else [])
                )
                if any(ao.get("orderType") in STOP_ORDER_TYPES
                       and ao.get("symbol") == sym for ao in algo):
                    _prot_set(sym, "protected")
                    continue

                # No stop found — mark unprotected (reconciliation will handle)
                _prot_set(sym, "unprotected")

            except Exception as e:
                # API failure — do NOT change state (keep existing, avoid false positive)
                logger.warning(f"Reseed cache: {sym} failed: {e} — keeping existing state")

        logger.info(f"Protection cache reseeded for {len(open_syms)} symbols")

    except Exception as e:
        logger.error(f"Protection cache reseed failed: {e}")

# ============================================================
# PID Lock — prevent double launch
# ============================================================
def _is_pid_alive(pid: int) -> bool:
    """Check if a process with the given PID is alive. Windows-compatible."""
    try:
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:
        pass
    # Fallback: use ctypes on Windows
    if os.name == "nt":
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if handle == 0:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    # POSIX fallback
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_lock() -> bool:
    """Create PID lock file. Returns False if another instance is running."""
    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
            if _is_pid_alive(old_pid):
                logger.error(f"❌ Another instance running (PID {old_pid}). Exiting.")
                return False
            else:
                logger.warning(f"Stale lock removed (PID {old_pid})")
        except (ValueError, OSError):
            pass
    LOCK_FILE.write_text(str(os.getpid()))
    return True


def release_lock():
    """Remove PID lock file."""
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


async def main():
    global conf, client, all_symbols, all_prices, signal_engine, risk_manager
    global position_manager, tg_channel, error_channel

    # PID lock
    if not acquire_lock():
        sys.exit(1)



    # Load .env file into environment (server-side secrets: TOKEN, DB_PASSWORD, etc.)
    # Priority: existing env vars > .env file > config.ini
    # Place .env next to config.ini (NOT committed to git).
    _env_file = Path(__file__).parent / ".env"
    if _env_file.exists():
        with open(_env_file) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k and _k not in os.environ:   # env vars already set take priority
                    os.environ[_k] = _v
        logger.info(f"✅ Loaded .env from {_env_file}")

    ini = configparser.ConfigParser()
    ini.read(Path(__file__).parent / "config.ini")

    # Prefer env vars for sensitive data, fallback to config.ini
    tg_channel = os.getenv("TG_CHANNEL", ini["TG"].get("channel", ""))
    error_channel = os.getenv("TG_ERROR_CHANNEL", ini["TG"].get("error_channel", ""))

    # DB connection — env vars take priority
    db_host = os.getenv("DB_HOST", ini["DB"]["host"])
    db_port = int(os.getenv("DB_PORT", ini["DB"]["port"]))
    db_user = os.getenv("DB_USER", ini["DB"]["user"])
    db_password = os.getenv("DB_PASSWORD", ini["DB"]["password"])
    db_name = os.getenv("DB_NAME", ini["DB"]["db"])

    # Connect DB
    await db.connect(db_host, db_port, db_user, db_password, db_name)
    await db.create_tables()
    await db.ensure_defaults()
    logger.info("✅ Connected to PostgreSQL")

    # Load config from DB — single source of truth (no config.json)
    conf = await db.load_config()

    # Init engines:
    # - LiveSignalEngine receives a typed StrategyConfig (correct field names guaranteed)
    # - RiskManager receives a plain dict (uses its own key names)
    signal_engine = LiveSignalEngine(conf.to_strategy_config())
    risk_manager  = RiskManager(conf.to_dict())

    # Check API keys
    if not conf.has_keys:
        logger.warning("⚠️ API keys not set. Running Telegram-only mode.")
        logger.warning("Set keys via ⚙️ Settings → Change API keys")
        logger.warning("After setting keys, RESTART the bot to begin trading.")
        await tg.run(ini)
        return  # tg.run blocks until bot stops; restart to trade

    # Init Binance client
    testnet = bool(conf.testnet) if conf.testnet is not None else ini.getboolean("BOT", "testnet")
    client = binance.Futures(
        conf.api_key, conf.api_secret,
        asynced=True, testnet=testnet,
    )
    logger.info(f"✅ Binance connected (testnet={testnet})")

    # Load symbols info
    all_symbols = await load_binance_symbols()
    all_prices = await get_all_prices()

    # Init position manager — all params from DB
    position_manager = PositionManager(
        client=client, all_symbols=all_symbols,
        partial_exit_pct=float(conf.partial_exit_pct or 0.10),
        leverage=int(conf.leverage or 20),
        margin_type="CROSSED",
    )
    logger.info(
        f"⚙️ Interval: {conf.interval} | Signal: {conf.signal_mode} | "
        f"Leverage: {int(conf.leverage or 20)}x | Risk: {float(conf.risk_per_trade or 0.01)*100:.1f}%"
    )

    # Sync positions
    await position_manager.sync_positions(positions)
    open_count = sum(1 for v in positions.values() if v)
    logger.info(f"✅ Positions synced: {open_count} open")

    # Startup reconciliation will run as a task after all services are up
    # (moved to after task creation so TG alerts are delivered — see below)

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal_module.SIGINT, signal_module.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: shutdown_event.set())
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    # Start parallel tasks
    # Note: listenkey_keepalive removed — WebsocketAsync already runs its own
    tasks = [
        asyncio.create_task(ws_reconnect_loop(), name="ws_reconnect"),
        asyncio.create_task(market_ws_reconnect_loop(), name="market_ws"),
        asyncio.create_task(signal_collector(), name="signal_collector"),
        asyncio.create_task(equity_logger(), name="equity_logger"),
        asyncio.create_task(health_watchdog(), name="health_watchdog"),
        asyncio.create_task(message_sender(), name="message_sender"),
        asyncio.create_task(error_sender(), name="error_sender"),
        asyncio.create_task(position_reconciliation(), name="reconciliation"),
    ]

    try:
        tasks.append(asyncio.create_task(
            tg.run(ini, client_ref=client), name="telegram"))
    except Exception:
        logger.warning("TG module not available")

    # Startup notification
    startup_text = (
        f"🟢 <b>Bot started</b>\n"
        f"Testnet: <b>{'ON' if testnet else 'OFF'}</b>\n"
        f"Positions: <b>{open_count}</b>\n"
        f"Symbols: <b>{len(await db.get_active_symbols())}</b>"
    )
    await _send_tg_message(tg_channel, startup_text)

    # Startup reconciliation AFTER tasks are running so TG alerts are delivered
    asyncio.create_task(startup_reconcile(client), name="startup_reconcile")

    # Wait for shutdown
    try:
        await shutdown_event.wait()
    except (KeyboardInterrupt, SystemExit):
        shutdown_event.set()

    logger.info("🛑 Shutting down...")

    # Cancel all tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Cleanup
    try:
        if client:
            await client.close()
    except Exception:
        pass
    await db.close()
    logger.info("✅ Cleanup done")


# ============================================================
# Helpers: shutdown-aware sleep + TG sender
# ============================================================
async def _shutdown_sleep(seconds: float) -> bool:
    """Sleep for `seconds`, but return True immediately if shutdown requested."""
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=seconds)
        return True  # shutdown requested
    except asyncio.TimeoutError:
        return False  # normal timeout


async def _send_tg_message(chat_id: str, text: str, reply_to_message_id: int | None = None):
    """Fire-and-forget TG message via queue.

    Pass reply_to_message_id to thread the message under the original
    trade-open notification (saved as trade.msg_id in the DB).
    """
    if not chat_id:
        return
    future = asyncio.get_running_loop().create_future()
    await message_queue.put((chat_id, text, future, reply_to_message_id))
    try:
        await asyncio.wait_for(future, timeout=10)
    except Exception:
        pass


def _tg_msg_link(chat_id: str, msg_id: int) -> str | None:
    """Build a deep-link URL to a specific Telegram channel message.

    Handles two formats:
    - '@username'  → https://t.me/username/{msg_id}
    - '-100XXXXXXX' numeric ID → https://t.me/c/{channel_id}/{msg_id}
    Returns None if chat_id is empty or msg_id is falsy.
    """
    if not chat_id or not msg_id:
        return None
    c = str(chat_id).strip()
    if c.startswith("@"):
        return f"https://t.me/{c[1:]}/{msg_id}"
    # Numeric ID: Telegram supergroup/channel IDs start with -100
    try:
        numeric = int(c)
        if numeric < 0:
            # Strip the leading -100 prefix to get the bare channel ID
            channel_id = str(abs(numeric))
            if channel_id.startswith("100"):
                channel_id = channel_id[3:]
            return f"https://t.me/c/{channel_id}/{msg_id}"
    except ValueError:
        pass
    return None


async def _fetch_klines_for(sym: str, interval: str, sem: asyncio.Semaphore) -> tuple[str, object]:
    """Fetch klines for one symbol under a shared concurrency semaphore."""
    async with sem:
        klines = await client.klines(sym, interval=interval, limit=200)
        return sym, utils.klines_to_dataframe(klines)


# ============================================================
# Signal Collector (every 4h candle close)
# ============================================================
async def signal_collector():
    """Main signal loop — runs at every candle close (interval from DB)."""
    global entries_this_bar, all_prices, signal_engine, risk_manager

    # Initial interval from current DB config
    _conf = await db.load_config()
    interval = str(_conf.interval or "4h")
    BAR_MS = {
        "1m":  60_000,    "5m":  300_000,  "15m": 900_000,  "30m": 1_800_000,
        "1h":  3_600_000, "4h":  14_400_000,"8h":  28_800_000,"1d":  86_400_000,
    }
    bar_duration_ms = BAR_MS.get(interval, 14_400_000)
    logger.info(f"📡 Signal collector started (interval={interval})")

    while not shutdown_event.is_set():
        await utils.wait_for_next_candle(interval, offset_seconds=10,
                                          shutdown_event=shutdown_event)
        if shutdown_event.is_set():
            break
        entries_this_bar = 0

        try:
            # Reload ALL config from DB — any TG change takes effect on next candle
            conf_fresh = await db.load_config()
            if not conf_fresh.trade_mode:
                logger.info("⏸ Trade mode OFF, skipping signals")
                continue

            # Rebuild engines from fresh config (all params from DB)
            interval = str(conf_fresh.interval or "4h")
            BAR_MS = {
                "1m":  60_000,    "5m":  300_000,  "15m": 900_000,  "30m": 1_800_000,
                "1h":  3_600_000, "4h":  14_400_000,"8h":  28_800_000,"1d":  86_400_000,
            }
            bar_duration_ms = BAR_MS.get(interval, 14_400_000)
            signal_engine                    = LiveSignalEngine(conf_fresh.to_strategy_config())
            risk_manager                     = RiskManager(conf_fresh.to_dict())
            position_manager.leverage        = int(conf_fresh.leverage or 20)
            position_manager.partial_exit_pct = float(conf_fresh.partial_exit_pct or 0.10)
            all_prices = await get_all_prices()
            await position_manager.sync_positions(positions)

            open_count = sum(1 for v in positions.values() if v)
            universe = await db.get_active_symbols()

            logger.info(f"🔍 Scanning {len(universe)} symbols | {open_count} open positions")

            # Fetch klines for all symbols + BTC
            symbol_frames = {}
            btc_frame = None

            try:
                btc_klines = await client.klines("BTCUSDT", interval=interval, limit=200)
                btc_frame = utils.klines_to_dataframe(btc_klines)
            except Exception as e:
                logger.error(f"BTC klines failed: {e}")
                continue

            # Parallel klines fetch with concurrency limit
            scan_symbols = [s for s in universe if not positions.get(s)]
            sem = asyncio.Semaphore(5)

            results = await asyncio.gather(
                *[_fetch_klines_for(s, interval, sem) for s in scan_symbols],
                return_exceptions=True,
            )
            for res in results:
                if isinstance(res, Exception):
                    logger.warning(f"klines fetch failed: {res}")
                else:
                    symbol_frames[res[0]] = res[1]

            if not symbol_frames:
                logger.info("No symbols to scan")
                continue

            # Compute signals (strategy logic untouched)
            signals = signal_engine.compute_signals(symbol_frames, btc_frame)
            logger.info(f"📊 {len(signals)} signals found")

            # Process signals
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            equity = await get_current_equity()

            for signal in signals:
                if not risk_manager.can_open_new(open_count, entries_this_bar):
                    break

                # Cooldown check
                last_close = await db.get_last_trade_close_time(signal.symbol)
                if risk_manager.is_on_cooldown(last_close, now_ms, bar_duration_ms):
                    continue

                # Position sizing
                sym_info = all_symbols.get(signal.symbol)
                if not sym_info:
                    continue

                sizing = risk_manager.compute_position_size(
                    equity=equity, atr=signal.atr, signal_score=signal.score,
                    entry_price=signal.entry_price, direction=signal.direction,
                    step_size=sym_info.step_size, tick_size=sym_info.tick_size,
                    min_notional=sym_info.notional, min_qty=sym_info.min_qty,
                    max_qty=sym_info.max_qty,
                    max_notional=equity * conf_fresh.leverage * 0.9,
                )
                if sizing is None:
                    continue

                # Open position
                async with symbol_locks[signal.symbol]:
                    if positions.get(signal.symbol):
                        continue
                    trade = await position_manager.open_position(
                        signal=signal, sizing=sizing,
                        interval=interval,
                        msg_queue=message_queue,
                        tg_channel=tg_channel,
                    )
                    if trade:
                        positions[signal.symbol] = True
                        entries_this_bar += 1
                        open_count += 1
                        # Register in active cache + subscribe markPrice WS stream
                        _active_trades[signal.symbol] = trade
                        _prot_set(signal.symbol, "protected")
                        asyncio.create_task(
                            _subscribe_price_stream(signal.symbol),
                            name=f"sub_{signal.symbol}"
                        )

            logger.info(f"✅ Signal cycle done: {entries_this_bar} new entries, {open_count} total open")

        except Exception as e:
            logger.error(f"Signal collector error: {e}\n{traceback.format_exc()}")
            await notify_error(f"Signal collector error: {e}")


# ============================================================
# WebSocket with Auto-Reconnect
# ============================================================
async def ws_reconnect_loop():
    """Connect to userdata WebSocket with automatic reconnection.

    websocket_userdata() is non-blocking — it starts the WS in the background
    and returns immediately. We use a ws_died event to detect when the socket
    actually disconnects, then sleep and reconnect.
    """
    global userdata_ws
    backoff = 5

    while not shutdown_event.is_set():
        ws_died = asyncio.Event()

        async def _on_error(ws, error):
            err_str = str(error)
            logger.error(f"❌ WS ERROR: {err_str[:200]}")
            # 401 = wrong keys / wrong endpoint — no point reconnecting
            if "401" in err_str or "Unauthorized" in err_str or "-2015" in err_str:
                msg = ("❌ WS 401 Unauthorized. Check API keys and testnet setting. "
                       "Fix in ⚙️ Settings, then restart bot.")
                logger.critical(msg)
                await notify_error(msg)
                shutdown_event.set()  # stop the bot entirely
            ws_died.set()

        try:
            userdata_ws = await client.websocket_userdata(
                on_message=ws_user_msg_safe, on_error=_on_error,
            )
            logger.info("✅ UserData WebSocket connected")
            backoff = 5  # reset on successful connect

            # Re-seed protection cache from REST (WS may have missed events during disconnect)
            asyncio.create_task(
                _reseed_protection_cache(), name="reseed_cache"
            )

            # Wait until either shutdown or WS dies
            done, _ = await asyncio.wait(
                [
                    asyncio.ensure_future(shutdown_event.wait()),
                    asyncio.ensure_future(ws_died.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if shutdown_event.is_set():
                break

            # WS died — will reconnect below
            logger.warning(f"🔄 WS dropped — reconnecting in {backoff}s...")

        except Exception as e:
            err_str = str(e)
            logger.error(f"WS connect failed: {e}")
            if "401" in err_str or "Unauthorized" in err_str or "-2015" in err_str:
                msg = "❌ WS 401 Unauthorized. Check API keys and testnet. Restart bot."
                logger.critical(msg)
                await notify_error(msg)
                return
            await notify_error(f"WebSocket connection failed: {e}")
            logger.warning(f"🔄 WS error — reconnecting in {backoff}s...")

        if shutdown_event.is_set():
            break
        if await _shutdown_sleep(backoff):
            break
        backoff = min(backoff * 2, 60)


async def ws_error(ws, error):
    logger.error(f"❌ WS ERROR: {error}")


async def ws_user_msg_safe(ws, msg):
    """Wrapper with error handling to prevent WS disconnect on handler crash."""
    try:
        await ws_user_msg(ws, msg)
    except Exception as e:
        logger.error(f"WS handler error: {e}\n{traceback.format_exc()}")


async def ws_user_msg(ws, msg):
    """Handle userdata WebSocket messages (order fills, position updates)."""
    global positions

    event_type = msg.get("e")

    if event_type == "ACCOUNT_UPDATE":
        for pos in msg["a"]["P"]:
            symbol = pos["s"]
            amt = float(pos["pa"])
            was_open = positions.get(symbol, False)
            is_open  = amt != 0.0
            positions[symbol] = is_open

            # ── Detect manually opened position (not created by this bot) ──
            if is_open and not was_open and symbol not in _active_trades:
                # Position appeared but bot has no record of opening it
                asyncio.create_task(
                    _handle_unexpected_position(symbol, amt),
                    name=f"unexpected_pos_{symbol}"
                )

            # ── Position just closed externally (manually or liquidation) ──
            if not is_open and was_open:
                if symbol in _active_trades:
                    logger.info(f"📡 {symbol}: position closed (ACCOUNT_UPDATE, external)")
                    _active_trades.pop(symbol, None)
                    asyncio.create_task(
                        _unsubscribe_price_stream(symbol),
                        name=f"unsub_{symbol}"
                    )
                # Clear protection cache — no position = no protection needed
                _prot_clear(symbol)

    elif event_type == "ORDER_TRADE_UPDATE":
        o = msg["o"]
        symbol = o["s"]
        status = o["X"]
        order_type = o.get("ot", o.get("o", ""))
        reduce = o.get("R", False)
        order_id = o["i"]  # extract early — used in CANCELED branch below

        # ── Update protection cache on order status changes ──
        if status == "NEW" and reduce and order_type in STOP_ORDER_TYPES:
            _prot_set(symbol, "protected")
            logger.info(f"📡 WS: {symbol} stop order {order_id} NEW → cache=protected")

        # Instant protection check: unexpected cancellation of a stop order
        if status in ("CANCELED", "EXPIRED"):
            if reduce and order_type in STOP_ORDER_TYPES:
                # Mark unprotected in cache
                _prot_set(symbol, "unprotected")
                logger.warning(f"📡 WS: {symbol} stop {order_id} {status} → cache=unprotected")
                # TP1 rollover cancels the old stop intentionally — ignore those
                if symbol not in _tp1_in_progress:
                    logger.warning(
                        f"⚠️ {symbol}: stop order {order_id} {status} unexpectedly. "
                        f"Scheduling protection check in 5s."
                    )
                    asyncio.create_task(
                        _check_protection_after_cancel(symbol),
                        name=f"prot_check_{symbol}",
                    )
            return  # CANCELED/EXPIRED orders have no fill data to process

        if status not in ("FILLED", "PARTIALLY_FILLED"):
            return

        # Stop order filled → position closing via stop
        if reduce and order_type in STOP_ORDER_TYPES:
            _prot_set(symbol, "unprotected")
            logger.info(f"📡 WS: {symbol} stop {order_id} FILLED → cache=unprotected")

        realized_profit = float(o.get("rp", 0))
        price = float(o.get("ap", 0))
        quantity = float(o.get("z", 0))

        # Update order in DB
        order, trade = await db.get_order_trade(order_id)
        if not order and reduce:
            trade = await db.get_open_trade(symbol)
            if trade:
                order = db.Order(
                    order_id=order_id, trade_id=trade.id,
                    symbol=symbol, time=o["T"],
                    side=o["S"] == "BUY", type=o["o"],
                    status=status, reduce=reduce,
                    price=price, quantity=quantity,
                    realized_profit=realized_profit,
                )
                await db.save_order(order)

        if order and trade:
            order.status = status
            order.price = price
            order.quantity = quantity
            order.realized_profit = realized_profit
            await db.save_order(order)
            await db.update_trade_result(trade.id)

            # (TP1 partial close is handled by price_monitor → position_manager)

            # Check for full close — re-read from DB to avoid race condition
            # (two WS messages for the same fill can arrive near-simultaneously)
            position_open = positions.get(symbol, False)
            is_exit_fill = (order and order.status == "FILLED" and order.type in ("LIMIT", "STOP_MARKET", "STOP"))
            if not position_open or is_exit_fill:
                fresh_trade = await db.get_open_trade(symbol)
                if fresh_trade is None:
                    # Already closed by a concurrent handler — skip
                    return

                # Determine close reason
                # On testnet, algo STOP creates a standard LIMIT order when triggered,
                # so order_type == "LIMIT" can mean EITHER take-profit OR stop-loss.
                # Use price comparison vs entry to disambiguate.
                if order_type in ("STOP_MARKET", "STOP"):
                    # Standard stop or algo stop (if Binance sends the original type)
                    if fresh_trade.take1_triggered and fresh_trade.breakeven_stop_price:
                        close_status = "CLOSED_BREAKEVEN"
                        exit_reason = "BREAKEVEN"
                    else:
                        close_status = "CLOSED_STOP"
                        exit_reason = "STOP"
                elif order_type == "LIMIT":
                    # Could be our LIMIT TP2 OR a LIMIT created by algo STOP trigger.
                    # Check: did we profit or lose? If price is on the wrong side
                    # of entry, it's definitely a stop, not a take.
                    fill_price = price  # from WS: o["ap"] = average fill price
                    target_stop = fresh_trade.breakeven_stop_price if (fresh_trade.take1_triggered and fresh_trade.breakeven_stop_price) else fresh_trade.stop_price
                    target_take = fresh_trade.take2_price
                    
                    if target_stop is not None and target_take is not None:
                        is_stop = abs(fill_price - target_stop) < abs(fill_price - target_take)
                    else:
                        is_long = fresh_trade.side  # True = LONG
                        if is_long:
                            # LONG: stop = price <= entry
                            is_stop = fill_price <= fresh_trade.entry_price
                        else:
                            # SHORT: stop = price >= entry
                            is_stop = fill_price >= fresh_trade.entry_price
                    
                    if is_stop:
                        if fresh_trade.take1_triggered and fresh_trade.breakeven_stop_price:
                            close_status = "CLOSED_BREAKEVEN"
                            exit_reason = "BREAKEVEN"
                        else:
                            close_status = "CLOSED_STOP"
                            exit_reason = "STOP"
                        logger.info(
                            f"{symbol}: LIMIT order at {fill_price} detected as STOP "
                            f"(entry={fresh_trade.entry_price}, algo stop trigger)"
                        )
                    else:
                        close_status = "CLOSED_TAKE"
                        exit_reason = "TAKE2"
                else:
                    close_status = "CLOSED_MARKET"
                    exit_reason = "MARKET"

                # Update result first, then close
                await db.update_trade_result(fresh_trade.id)
                fresh_trade = await db.get_open_trade(symbol)  # refresh result field
                result_pnl = 0.0
                if fresh_trade:
                    result_pnl = fresh_trade.result or 0.0
                    r_mult = result_pnl / fresh_trade.initial_risk if fresh_trade.initial_risk else 0

                    # Cancel surviving protective orders after a stop/take fill.
                    # On testnet, stops are algo orders — cancel_order() with their
                    # synthetic ID returns -2011.  Use belt-and-suspenders instead:
                    #   a) cancel_open_orders() — wipes all remaining standard orders
                    #   b) enumerate + cancel all algo orders for this symbol
                    try:
                        await client.cancel_open_orders(symbol=symbol)
                    except Exception:
                        pass
                    try:
                        algo_raw = await client.get_algo_orders()
                        all_algo = (
                            algo_raw.get("algoOrders", [])
                            if isinstance(algo_raw, dict)
                            else (algo_raw if isinstance(algo_raw, list) else [])
                        )
                        for ao in all_algo:
                            if ao.get("symbol") == symbol:
                                try:
                                    await client.cancel_algo_order(algoId=ao["algoId"])
                                    logger.info(f"{symbol}: cancelled algo order {ao['algoId']} on close")
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    # Mark surviving tracked orders as CANCELED in DB
                    for active_order in await db.get_active_reduce_orders(fresh_trade.id):
                        if active_order.order_id == order_id:
                            continue
                        await db.save_order(db.Order(
                            order_id=active_order.order_id,
                            trade_id=fresh_trade.id,
                            symbol=symbol,
                            time=o["T"],
                            side=active_order.side,
                            type=active_order.type,
                            status="CANCELED",
                            reduce=active_order.reduce,
                            price=active_order.price,
                            quantity=active_order.quantity,
                            realized_profit=active_order.realized_profit or 0.0,
                        ))

                    await db.close_trade(fresh_trade.id, close_status, exit_reason)
                    await db.update_trade(fresh_trade.id, r_multiple=r_mult)

                    # Telegram notification
                    emoji = {"STOP": "⛔", "TAKE2": "🎯", "BREAKEVEN": "🔄", "MARKET": "📉"}
                    tp1_note = " (после TP1)" if fresh_trade.take1_triggered else ""
                    exit_price_str = f"{price:.4f}" if price else "—"
                    text = (
                        f"{emoji.get(exit_reason, '📉')} "
                        f"<b>{'LONG' if fresh_trade.side else 'SHORT'} {symbol}</b> закрыт{tp1_note}\n"
                        f"Причина: <b>{exit_reason}</b>\n"
                        f"Вход: <b>{fresh_trade.entry_price}</b> | Выход: <b>{exit_price_str}</b>\n"
                        f"PnL: <b>{result_pnl:+.4f} USDT</b>\n"
                        f"R-кратное: <b>{r_mult:+.2f}R</b>"
                    )
                    if fresh_trade.msg_id:
                        logger.info(f"{symbol}: replying to opening msg_id={fresh_trade.msg_id} on close")
                        # Append clickable link to original open message (works in any channel type)
                        orig_link = _tg_msg_link(tg_channel, fresh_trade.msg_id)
                        if orig_link:
                            text += f"\n↩️ <a href='{orig_link}'>Открытие позиции</a>"
                    else:
                        logger.warning(
                            f"{symbol}: msg_id=None in DB — "
                            f"close notification won't be threaded under opening message"
                        )
                    await _send_tg_message(
                        tg_channel, text,
                        reply_to_message_id=fresh_trade.msg_id,
                    )
                    logger.info(f"{'✅' if result_pnl > 0 else '❌'} {symbol} closed: {exit_reason} @ {exit_price_str} PnL={result_pnl:+.4f}")
                    # Remove from active cache + unsubscribe price stream
                    _active_trades.pop(symbol, None)
                    asyncio.create_task(
                        _unsubscribe_price_stream(symbol), name=f"unsub_{symbol}"
                    )


async def _check_protection_after_cancel(symbol: str) -> None:
    """WS-triggered protection check after unexpected stop order cancellation.

    Waits 5 seconds grace period so the bot can place a replacement stop
    (e.g. normal flow after TP1 partial close), then verifies protection.
    Uses cache first, REST-verify as safety net before force-close.
    """
    await asyncio.sleep(5)  # Grace period for intentional replacement

    try:
        # Check cache first — TP1 rollover sets "protected" within 5s
        if _prot_is_protected(symbol):
            logger.info(f"✅ {symbol}: cache=protected after cancel grace period (replacement placed)")
            return

        # Check if position still exists
        pos_raw = await client.get_position_risk(symbol=symbol)
        pos_info = next((p for p in pos_raw if p["symbol"] == symbol), None)
        if not pos_info or abs(float(pos_info["positionAmt"])) == 0:
            return  # Position already closed — no action needed

        amt = float(pos_info["positionAmt"])

        # REST-verify: 3 retries, skip force-close on API failure
        if not await _rest_verify_no_protection(symbol, client):
            logger.info(f"✅ {symbol}: REST verify found stop (or couldn't verify) — keeping position")
            return

        # Confirmed: no stop after grace period + REST verification
        logger.critical(
            f"🚨 {symbol}: stop cancelled, no replacement confirmed after 5s + REST verify. "
            f"Force-closing position (qty={amt})."
        )
        await _force_close_position(
            symbol, amt,
            "WS: stop order cancelled with no replacement (REST-confirmed)",
            client,
        )

    except Exception as e:
        logger.error(f"{symbol}: _check_protection_after_cancel failed: {e}")


# ============================================================
# Market Data WebSocket — real-time TP1 detection via markPrice
# ============================================================

async def _subscribe_price_stream(symbol: str) -> None:
    """Subscribe to markPrice@1s stream for a symbol."""
    global market_ws
    if market_ws is None:
        return
    stream = f"{symbol.lower()}@markPrice@1s"
    try:
        await market_ws.subscribe([stream])
        logger.info(f"📡 Subscribed markPrice stream: {symbol}")
    except Exception as e:
        logger.warning(f"{symbol}: subscribe markPrice failed: {e}")


async def _unsubscribe_price_stream(symbol: str) -> None:
    """Unsubscribe from markPrice@1s stream for a symbol."""
    global market_ws
    if market_ws is None:
        return
    stream = f"{symbol.lower()}@markPrice@1s"
    try:
        await market_ws.unsubscribe([stream])
        logger.info(f"📡 Unsubscribed markPrice stream: {symbol}")
    except Exception as e:
        logger.warning(f"{symbol}: unsubscribe markPrice failed: {e}")


async def _execute_tp1(trade, price: float) -> None:
    """Execute TP1 partial close + move stop to breakeven.
    Called from WS markPrice handler when TP1 price is hit.

    CRITICAL: cache (_active_trades) is updated IMMEDIATELY after DB write
    to prevent infinite log-spam if partial_close_and_move_stop fails.
    Without this, ws_market_msg keeps seeing take1_triggered=False in cache
    while DB has True, causing a new task every tick (1/sec).
    """
    symbol = trade.symbol
    async with symbol_locks[symbol]:
        # Re-read from DB inside lock to avoid race
        fresh_trade = await db.get_open_trade(symbol)
        if not fresh_trade or fresh_trade.take1_triggered:
            return

        # ── Mark TP1 as triggered in BOTH DB and cache FIRST ──────────
        # This prevents the ws_market_msg → _execute_tp1 spam loop even
        # if the exchange call below fails.
        await db.update_trade(fresh_trade.id, take1_triggered=True)
        fresh_trade.take1_triggered = True
        _active_trades[symbol] = fresh_trade

        _tp1_in_progress.add(symbol)
        try:
            # ── Retry partial close with exponential backoff ──────────
            TP1_MAX_RETRIES = 3
            ok = False
            last_err = None
            for attempt in range(TP1_MAX_RETRIES):
                try:
                    ok = await position_manager.partial_close_and_move_stop(fresh_trade)
                    if ok:
                        break
                    # partial_close returned False (e.g. position already closed)
                    last_err = "partial_close returned False"
                except Exception as e:
                    last_err = str(e)
                    logger.error(
                        f"{symbol}: TP1 partial close attempt {attempt+1}/{TP1_MAX_RETRIES} "
                        f"failed: {e}"
                    )
                if attempt < TP1_MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** (attempt + 1))  # 2s, 4s

            if ok:
                _prot_set(symbol, "protected")  # new stop placed by TP1 rollover

                pct = int((position_manager.partial_exit_pct or 0.1) * 100)
                sym_info = all_symbols.get(symbol)
                be_tick = sym_info.tick_size if sym_info else 4
                be_stop = round(fresh_trade.entry_price, be_tick)
                tp1_text = (
                    f"🎯 <b>{symbol}</b> TP1 достигнут @ {price:.4f}\n"
                    f"Закрыто <b>{pct}%</b> позиции\n"
                    f"Стоп перенесён на безубыток: <b>{be_stop}</b>\n"
                    f"TP2 активен: <b>{fresh_trade.take2_price}</b>"
                )
                orig_link = _tg_msg_link(tg_channel, fresh_trade.msg_id)
                if orig_link:
                    tp1_text += f"\n↩️ <a href='{orig_link}'>Открытие позиции</a>"
                await _send_tg_message(
                    tg_channel, tp1_text,
                    reply_to_message_id=fresh_trade.msg_id,
                )
            else:
                # All retries exhausted — TP1 is marked but NOT executed.
                # Alert operator: stop is still at original level, no partial close.
                logger.critical(
                    f"🚨 {symbol}: TP1 marked but partial close FAILED after "
                    f"{TP1_MAX_RETRIES} retries: {last_err}"
                )
                await notify_error(
                    f"🚨 <b>{symbol}</b>: TP1 triggered but partial close failed!\n"
                    f"Stop is still at original level ({fresh_trade.stop_price}).\n"
                    f"Error: {last_err}\n"
                    f"Action: verify position manually or restart bot."
                )
        finally:
            _tp1_in_progress.discard(symbol)


async def ws_market_msg(ws, msg) -> None:
    """Handle market data WebSocket messages (markPrice ticks).

    Replaces REST polling (start_price_monitor). Zero-latency TP1 detection.
    Combined stream format: {"stream": "...", "data": {...}}
    """
    # Unwrap combined stream envelope
    data = msg.get("data", msg) if isinstance(msg, dict) else msg
    if not isinstance(data, dict):
        return

    event = data.get("e")
    if event != "markPriceUpdate":
        return

    symbol = data.get("s")
    if not symbol:
        return

    mark_price_str = data.get("p") or data.get("mp")
    if not mark_price_str:
        return

    try:
        price = float(mark_price_str)
    except (ValueError, TypeError):
        return

    # Fast path: use in-memory cache — no DB call on every tick
    trade = _active_trades.get(symbol)
    if trade is None or trade.take1_triggered:
        return
    if symbol in _tp1_in_progress:
        return

    direction = 1 if trade.side else -1
    tp1_hit = (price >= trade.take1_price if direction == 1 else price <= trade.take1_price)
    if tp1_hit:
        logger.info(f"📡 {symbol}: TP1 hit via WS @ {price:.4f} (tp1={trade.take1_price})")
        trade.take1_triggered = True  # Mark immediately to prevent duplicate task loops on subsequent ticks
        asyncio.create_task(_execute_tp1(trade, price), name=f"tp1_{symbol}")


async def ws_market_msg_safe(ws, msg) -> None:
    try:
        await ws_market_msg(ws, msg)
    except Exception as e:
        logger.error(f"Market WS handler error: {e}")


async def market_ws_reconnect_loop() -> None:
    """Connect to market data WebSocket with auto-reconnect.

    Starts with empty stream list, then subscribes per-symbol streams
    dynamically as positions are opened/closed.
    """
    global market_ws
    backoff = 5

    while not shutdown_event.is_set():
        ws_died = asyncio.Event()

        async def _on_market_error(ws, error):
            logger.warning(f"Market WS error: {error}")
            ws_died.set()

        try:
            # Start with empty stream list — subscriptions are added dynamically
            market_ws = await client.websocket(
                stream=[],
                on_message=ws_market_msg_safe,
                on_error=_on_market_error,
            )
            logger.info("✅ Market data WebSocket connected (markPrice streams)")
            backoff = 5

            # Re-subscribe for all currently active trades after (re)connect
            for sym in list(_active_trades.keys()):
                await _subscribe_price_stream(sym)

            done, _ = await asyncio.wait(
                [
                    asyncio.ensure_future(shutdown_event.wait()),
                    asyncio.ensure_future(ws_died.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_event.is_set():
                break
            logger.warning(f"🔄 Market WS dropped — reconnecting in {backoff}s...")

        except Exception as e:
            logger.error(f"Market WS connect failed: {e}")

        if shutdown_event.is_set():
            break
        if await _shutdown_sleep(backoff):
            break
        backoff = min(backoff * 2, 60)


async def _handle_unexpected_position(symbol: str, amt: float) -> None:
    """Alert operator about a manually opened position not tracked by this bot."""
    await asyncio.sleep(3)  # brief delay to let bot's own open_position finish if racing
    if symbol in _active_trades:
        return  # bot opened it — race condition resolved
    trade = await db.get_open_trade(symbol)
    if trade is not None:
        return  # DB record exists — fine
    logger.warning(f"⚠️ {symbol}: MANUAL position detected (qty={amt}) — not in DB!")
    await notify_error(
        f"⚠️ <b>Ручная позиция</b>: <b>{symbol}</b> qty={amt:+.4f}\n"
        f"Бот её не открывал и не отслеживает.\n"
        f"Закройте вручную или используйте /close из TG."
    )
    # Subscribe price stream even for manual positions (protection monitoring)
    await _subscribe_price_stream(symbol)


# ============================================================
# Helpers
# ============================================================
async def load_binance_symbols() -> dict:
    """Load symbol info from Binance."""
    try:
        symbols = {}
        info = await client.exchange_info()
        for s in info.get("symbols", []):
            if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING":
                sym = binance.SymbolFutures(s)
                symbols[sym.symbol] = sym
        return symbols
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        return {}


async def get_all_prices() -> dict:
    """Get all current prices."""
    try:
        tickers = await client.ticker_price()
        return {t["symbol"]: float(t["price"]) for t in tickers}
    except Exception as e:
        logger.error(f"Failed to load prices: {e}")
        return {}


async def get_current_equity() -> float:
    """Get current equity from Binance account (totalWalletBalance = real USDT balance).
    If manual_equity is set in DB (>0), use that value instead — useful for testnet
    or small-deposit simulation where Binance balance is artificially large.
    """
    global client
    # Check manual equity override first
    try:
        conf = await db.load_config()
        manual = float(conf.manual_equity or 0)
        if manual > 0:
            logger.debug(f"💰 Equity: using manual override = ${manual:.2f}")
            return manual
    except Exception:
        pass
    # Fetch real balance from Binance
    if client is not None:
        try:
            account = await client.account()
            balance = float(account.get("totalWalletBalance", 0))
            if balance > 0:
                logger.debug(f"💰 Equity: using Binance balance = ${balance:.2f}")
                return balance
        except Exception as e:
            logger.warning(f"⚠️ Cannot fetch Binance balance: {e}, using PnL fallback")
    # Fallback: sum of all realized PnL
    total_pnl = await db.get_total_realized_pnl()
    return max(total_pnl, 100.0)  # at least $100 to avoid zero sizing


# ============================================================
# Background Tasks
# ============================================================
async def equity_logger():
    """Log equity every hour."""
    while not shutdown_event.is_set():
        if await _shutdown_sleep(3600):
            break
        try:
            wallet_balance = 0.0
            unrealized_pnl = 0.0
            
            global client
            if client is not None:
                try:
                    account = await client.account()
                    wallet_balance = float(account.get("totalWalletBalance", 0))
                    unrealized_pnl = float(account.get("totalUnrealizedProfit", 0))
                except Exception as e:
                    logger.warning(f"⚠️ Cannot fetch Binance balance for hourly logging: {e}")
            
            # If fetch failed or not connected, fall back to DB realized PnL
            if wallet_balance <= 0:
                total_pnl = await db.get_total_realized_pnl()
                wallet_balance = max(total_pnl, 100.0)
                unrealized_pnl = 0.0

            open_count = sum(1 for v in positions.values() if v)
            await db.log_equity(wallet_balance, open_count, unrealized_pnl)
            logger.info(f"💰 Equity Logged: ${wallet_balance:.2f} (uPnL {unrealized_pnl:+.2f}) | {open_count} positions")
        except Exception as e:
            logger.error(f"Equity logger error: {e}")


async def health_watchdog():
    """Check bot health every 5 minutes."""
    while not shutdown_event.is_set():
        if await _shutdown_sleep(300):
            break
        try:
            await client.ping()
            logger.debug("❤️ Binance ping OK")
        except Exception as e:
            logger.error(f"🚨 Health check failed: {e}")
            await notify_error(f"Health check failed: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation helpers
# ──────────────────────────────────────────────────────────────────────────────

# STOP_ORDER_TYPES / TAKE_ORDER_TYPES are defined at module level (top of file)


async def _rest_verify_no_protection(symbol: str, bc) -> bool:
    """REST-based verification that a position truly has no stop protection.

    Called ONLY when the cache says 'no stop' — as a safety net before force-close.
    Returns True if the position is CONFIRMED unprotected (safe to force-close).
    Returns False if stop IS found or if API fails (never force-close on error).
    """
    for attempt in range(3):
        try:
            # Check standard orders
            std = await bc.get_orders(symbol=symbol)
            std = std if isinstance(std, list) else []
            if any(o.get("type") in STOP_ORDER_TYPES for o in std):
                logger.info(f"REST verify: {symbol} HAS stop (cache was stale), updating cache")
                _prot_set(symbol, "protected")
                return False

            # Check algo orders
            algo_raw = await bc.get_algo_orders(symbol=symbol)
            algo = (
                algo_raw.get("algoOrders", []) if isinstance(algo_raw, dict)
                else (algo_raw if isinstance(algo_raw, list) else [])
            )
            algo = [ao for ao in algo if ao.get("symbol") == symbol]
            if any(ao.get("orderType") in STOP_ORDER_TYPES for ao in algo):
                logger.info(f"REST verify: {symbol} HAS algo stop (cache was stale), updating cache")
                _prot_set(symbol, "protected")
                return False

            # Confirmed: no stop protection
            logger.warning(f"REST verify: {symbol} CONFIRMED no stop (attempt {attempt+1})")
            return True

        except Exception as e:
            if attempt < 2:
                logger.warning(f"REST verify {symbol} attempt {attempt+1} failed: {e}, retrying...")
                await asyncio.sleep(2)
            else:
                logger.error(
                    f"REST verify {symbol} FAILED after 3 attempts: {e}. "
                    f"SKIPPING force-close to avoid false positive."
                )
                return False  # Never force-close if we can't verify

    return False


async def _force_close_position(sym: str, amt: float, reason: str, bc) -> None:
    """Cancel all open orders for a symbol then MARKET-close the position.

    Used as the last resort when a position is unsafe (naked, unprotected, or orphaned).
    Sends a TG notification regardless of outcome.
    """
    close_side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)

    # 1. Cancel standard open orders
    try:
        await bc.cancel_open_orders(symbol=sym)
        logger.info(f"{sym}: open orders cancelled (force-close)")
    except Exception as e:
        logger.warning(f"{sym}: cancel_open_orders skipped: {e}")

    # 2. Cancel algo open orders
    try:
        algo_raw = await bc.get_algo_orders()
        all_algo = (
            algo_raw.get("algoOrders", [])
            if isinstance(algo_raw, dict)
            else (algo_raw if isinstance(algo_raw, list) else [])
        )
        for ao in all_algo:
            if ao.get("symbol") == sym:
                try:
                    await bc.cancel_algo_order(algoId=ao["algoId"])
                    logger.info(f"{sym}: algo order {ao['algoId']} cancelled")
                except Exception:
                    pass
    except Exception:
        pass

    # 3. Format quantity using symbol info if available
    sym_info = position_manager.all_symbols.get(sym) if position_manager else None
    step_size = sym_info.step_size if sym_info else 0
    qty_str = f"{qty:.{max(0, int(step_size))}f}"

    # 4. Market close (with -2022 reduceOnly fallback for testnet)
    try:
        await bc.new_order(
            symbol=sym, side=close_side, type="MARKET",
            quantity=qty_str, reduceOnly="true",
        )
        logger.info(f"⚡ {sym}: force-closed qty={qty_str} | reason: {reason}")
        close_msg = f"⚡ <b>{sym}</b>: auto-closed.\nReason: {reason}\nqty={qty_str}"
        await _send_tg_message(tg_channel, close_msg)
        await notify_error(close_msg)
    except Exception as e:
        if "-2022" in str(e):
            # Testnet: algo stop holds full reduceOnly quota → retry without flag
            logger.warning(f"{sym}: reduceOnly rejected (-2022), retrying without flag")
            try:
                await bc.new_order(
                    symbol=sym, side=close_side, type="MARKET",
                    quantity=qty_str,
                )
                logger.info(f"⚡ {sym}: force-closed (no reduceOnly) qty={qty_str} | reason: {reason}")
                close_msg = f"⚡ <b>{sym}</b>: auto-closed.\nReason: {reason}\nqty={qty_str}"
                await _send_tg_message(tg_channel, close_msg)
                await notify_error(close_msg)
            except Exception as e2:
                logger.critical(f"{sym}: FORCE CLOSE FAILED (both attempts): {e2}")
                err_msg = (
                    f"🚨 <b>{sym}</b>: AUTO-CLOSE FAILED — manual action needed!\n"
                    f"Reason: {reason}\nError: {e2}"
                )
                await _send_tg_message(tg_channel, err_msg)
                await notify_error(err_msg)
        else:
            logger.critical(f"{sym}: FORCE CLOSE FAILED: {e}")
            err_msg = (
                f"🚨 <b>{sym}</b>: AUTO-CLOSE FAILED — manual action needed!\n"
                f"Reason: {reason}\nError: {e}"
            )
            await _send_tg_message(tg_channel, err_msg)
            await notify_error(err_msg)

    # Always clear protection cache for this symbol
    _prot_clear(sym)


async def _try_reprotect_position(sym: str, trade: db.Trade) -> bool:
    """Attempt to re-place stop+take orders for a position that lost protection.

    Uses the trade's DB data to determine correct stop/take prices and
    remaining quantity from exchange.  Returns True if at least the stop
    was placed successfully.
    """
    try:
        direction = 1 if trade.side else -1
        close_side = "SELL" if direction == 1 else "BUY"

        # Get current position size from exchange
        pos_list = await client.get_position_risk(symbol=sym)
        pos_info = next((p for p in pos_list if p["symbol"] == sym), None)
        if not pos_info or abs(float(pos_info["positionAmt"])) == 0:
            logger.warning(f"{sym}: position already flat, skip re-protect")
            return False
        remaining = abs(float(pos_info["positionAmt"]))

        sym_info = all_symbols.get(sym)
        if not sym_info:
            logger.error(f"{sym}: no symbol info for re-protect")
            return False

        step_size = sym_info.step_size
        tick_size = sym_info.tick_size
        qty_str = position_manager._fmt_qty(remaining, step_size)

        # Determine stop price: breakeven if TP1 triggered, original otherwise
        if trade.take1_triggered and trade.breakeven_stop_price:
            stop_price = trade.breakeven_stop_price
        else:
            stop_price = trade.stop_price
        stop_str = position_manager._fmt_price(stop_price, tick_size)

        # Cancel any remaining orders first (clean slate)
        try:
            await client.cancel_open_orders(symbol=sym)
        except Exception:
            pass
        try:
            algo_raw = await client.get_algo_orders(symbol=sym)
            open_algo = (
                algo_raw.get("algoOrders", [])
                if isinstance(algo_raw, dict)
                else (algo_raw if isinstance(algo_raw, list) else [])
            )
            for ao in open_algo:
                if ao.get("symbol") == sym and ao.get("algoId"):
                    try:
                        await client.cancel_algo_order(algoId=ao["algoId"])
                    except Exception:
                        pass
        except Exception:
            pass

        # Place new stop
        stop_ok = False
        try:
            new_stop = await position_manager._place_stop_market(
                symbol=sym, side=close_side,
                qty_str=qty_str, stop_str=stop_str,
            )
            if trade.id:
                await position_manager._save_order_snapshot(trade.id, sym, new_stop)
            stop_ok = True
            logger.info(f"✅ {sym}: re-placed STOP @ {stop_str}")
        except Exception as e:
            logger.error(f"❌ {sym}: re-place STOP failed: {e}")

        # Place new take2 (if TP1 not yet triggered, or as TP2 target)
        take_price = trade.take2_price
        if take_price:
            take_str = position_manager._fmt_price(take_price, tick_size)
            try:
                new_take = await position_manager._place_limit_take(
                    symbol=sym, side=close_side,
                    qty_str=qty_str, price_str=take_str,
                )
                if trade.id:
                    await position_manager._save_order_snapshot(trade.id, sym, new_take)
                logger.info(f"✅ {sym}: re-placed TAKE2 @ {take_str}")
            except Exception as e:
                logger.warning(f"⚠️ {sym}: re-place TAKE2 failed: {e}")

        return stop_ok

    except Exception as e:
        logger.error(f"{sym}: _try_reprotect_position failed: {e}")
        return False


async def startup_reconcile(binance_client) -> None:
    """Run once on startup: per-symbol cross-check of exchange vs DB.

    For every symbol, checks that:
      - Position on exchange   → has at least one protective order (stop or take)
      - Position on exchange   → tracked in DB as open trade
      - Open order on exchange → has a corresponding open position
      - Open trade in DB       → has a real position on exchange

    Orphan orders  (order exists, no position)  → cancelled automatically.
    Naked position (position, no orders)        → critical alert to TG.
    Unprotected    (position, no stop)          → warning alert.
    Stale DB trade (DB open, exchange flat)     → closed in DB.
    """
    logger.info("🔍 Startup reconciliation: per-symbol exchange vs DB check...")
    try:
        # ── Fetch exchange state ──────────────────────────────────────────
        binance_pos_list = await binance_client.get_position_risk()
        binance_open_pos = {
            p["symbol"]: float(p["positionAmt"])
            for p in binance_pos_list
            if abs(float(p["positionAmt"])) > 0 and "_" not in p["symbol"]
        }

        # ── Fetch DB state ────────────────────────────────────────────────
        db_trades = await db.get_all_open_trades()
        db_by_sym = {t.symbol: t for t in db_trades}

        # NOTE: _active_trades cache is seeded AFTER stale-trade detection (section 3)
        # to avoid ghost entries for positions closed while the bot was offline.

        # Symbols to scan for orphan orders: open positions + DB trades + universe (20 symbols).
        # NOTE: do NOT use position_manager.all_symbols here — that set contains 400+ Binance
        # perpetual symbols and would trigger 400+ concurrent get_orders() calls at startup.
        # The universe (20 TV-ranked symbols) is sufficient: orphan orders can only appear on
        # symbols the bot has ever traded, which are always a subset of the universe.
        universe_syms = set(await db.get_active_symbols())
        syms_to_scan = set(binance_open_pos) | set(db_by_sym) | universe_syms

        # Fetch open orders per-symbol concurrently (library requires symbol per call)
        # NOTE: get_orders() = /fapi/v1/openOrders (all open orders for symbol)
        #       get_open_orders() = /fapi/v1/openOrder (single order by ID — WRONG)
        async def _fetch_orders(sym: str) -> list:
            try:
                result = await asyncio.wait_for(
                    binance_client.get_orders(symbol=sym), timeout=5.0
                )
                return result if isinstance(result, list) else []
            except Exception:
                return []

        order_results = await asyncio.gather(
            *[_fetch_orders(s) for s in syms_to_scan]
        )
        binance_orders: list = [o for sub in order_results for o in sub]

        # Collect tracked order IDs per symbol from DB
        tracked_order_ids: dict[str, set] = {}
        for t in db_trades:
            ids = set()
            for o in (await db.get_active_reduce_orders(t.id) or []):
                ids.add(o.order_id)
            tracked_order_ids[t.symbol] = ids

        # Fetch algo orders
        algo_orders: list = []
        try:
            algo_raw = await binance_client.get_algo_orders()
            algo_orders = (
                algo_raw.get("algoOrders", [])
                if isinstance(algo_raw, dict)
                else (algo_raw if isinstance(algo_raw, list) else [])
            )
        except Exception as ae:
            logger.warning(f"Algo orders fetch skipped: {ae}")

        # Index exchange orders by symbol
        exchange_orders_by_sym: dict[str, list] = {}
        for o in binance_orders:
            sym = o.get("symbol", "")
            if "_" not in sym:
                exchange_orders_by_sym.setdefault(sym, []).append(o)

        exchange_algo_by_sym: dict[str, list] = {}
        for ao in algo_orders:
            sym = ao.get("symbol", "")
            if sym and "_" not in sym:
                exchange_algo_by_sym.setdefault(sym, []).append(ao)

        cancelled = 0
        alerts = []

        # ── 1. Orders without open position → orphan, cancel ─────────────
        all_order_syms = set(exchange_orders_by_sym) | set(exchange_algo_by_sym)
        for sym in all_order_syms:
            if sym not in binance_open_pos:
                # Orders exist but NO open position on exchange → orphan
                for o in exchange_orders_by_sym.get(sym, []):
                    o_type = o.get('type', '?')
                    o_id   = o.get('orderId', '?')
                    logger.warning(
                        f"🔧 Orphan order [{o_type} id={o_id}] "
                        f"on {sym} — no open position. Cancelling."
                    )
                    try:
                        await binance_client.cancel_order(symbol=sym, orderId=o["orderId"])
                        cancelled += 1
                        await _send_tg_message(tg_channel,
                            f"🔧 <b>Startup cleanup</b>\n"
                            f"{sym}: cancelled orphan <b>{o_type}</b> order (id={o_id})\n"
                            f"Reason: no open position on exchange"
                        )
                    except Exception as ce:
                        logger.error(f"  ❌ cancel_order failed: {ce}")

                for ao in exchange_algo_by_sym.get(sym, []):
                    ao_type = ao.get('orderType', '?')
                    ao_id   = ao.get('algoId', '?')
                    logger.warning(
                        f"🔧 Orphan algo order [{ao_type} algoId={ao_id}] "
                        f"on {sym} — no open position. Cancelling."
                    )
                    try:
                        await binance_client.cancel_algo_order(algoId=ao["algoId"])
                        cancelled += 1
                        await _send_tg_message(tg_channel,
                            f"🔧 <b>Startup cleanup</b>\n"
                            f"{sym}: cancelled orphan <b>{ao_type}</b> algo stop (id={ao_id})\n"
                            f"Reason: no open position on exchange"
                        )
                    except Exception as ce:
                        logger.error(f"  ❌ cancel_algo_order failed: {ce}")

        # ── 2. Open positions: check protection & DB tracking ─────────────
        for sym, amt in binance_open_pos.items():
            std_orders  = exchange_orders_by_sym.get(sym, [])
            algo_sym    = exchange_algo_by_sym.get(sym, [])

            close_side = "SELL" if amt > 0 else "BUY"  # opposite side closes the position
            has_stop = (
                any(o.get("type") in STOP_ORDER_TYPES for o in std_orders) or
                any(ao.get("orderType") in STOP_ORDER_TYPES for ao in algo_sym)
            )
            # Use side-based detection: take order is always on the close side
            # (reduceOnly is unreliable on testnet — often returned as False)
            has_take = (
                any(o.get("type") in TAKE_ORDER_TYPES and o.get("side") == close_side for o in std_orders) or
                any(ao.get("orderType") in TAKE_ORDER_TYPES for ao in algo_sym)
            )
            in_db = sym in db_by_sym

            # ── Seed protection cache from startup data ────────────
            if has_stop:
                _prot_set(sym, "protected")
            else:
                _prot_set(sym, "unprotected")

            # Position not tracked in DB at all
            if not in_db:
                if has_stop and has_take:
                    # Has both orders but no DB entry → likely bot crashed between
                    # order placement and DB write in open_position().
                    # DO NOT auto-close — this may be a valid trade.
                    msg = (
                        f"⚠️ {sym}: position with stop+take found but NOT in DB.\n"
                        f"Likely crash during trade open. Verify manually!"
                    )
                    logger.warning(msg.replace("\n", " "))
                    alerts.append(msg)
                else:
                    # No DB + no full protection → orphaned position.
                    # Covers: a) no orders at all  b) only take, no stop  c) only stop, no take
                    logger.critical(
                        f"🚨 {sym}: orphan position (not in DB, "
                        f"has_stop={has_stop} has_take={has_take}). Auto-closing."
                    )
                    await _force_close_position(
                        sym, amt, "orphan: not in DB and no full protection", binance_client
                    )

                # Orphan handled above (warned or already closed).
                # Skip DB-based protection checks — they apply to tracked trades only.
                continue

            # ── DB-tracked position: verify it has stop protection ────────
            trade = db_by_sym[sym]

            # 1. Verify side matches (prevent glitched long vs short state)
            db_side_is_long = bool(trade.side)
            exchange_side_is_long = (amt > 0)
            if db_side_is_long != exchange_side_is_long:
                logger.critical(
                    f"🚨 {sym}: POSITION SIDE MISMATCH! Exchange={'LONG' if exchange_side_is_long else 'SHORT'}, "
                    f"DB={'LONG' if db_side_is_long else 'SHORT'}. Emergency closing position."
                )
                await _send_tg_message(tg_channel,
                    f"🚨 <b>{sym}: Position Side Mismatch!</b>\n"
                    f"Биржа: <b>{'LONG' if exchange_side_is_long else 'SHORT'}</b> ({amt})\n"
                    f"БД: <b>{'LONG' if db_side_is_long else 'SHORT'}</b> ({trade.quantity})\n"
                    f"Аварийно закрываем позицию на бирже."
                )
                await _force_close_position(sym, amt, f"side mismatch with DB trade #{trade.id}", binance_client)
                continue

            # 2. Verify protective orders
            # Position has NO protective orders whatsoever → try re-protect first
            if not has_stop and not has_take:
                logger.critical(f"🚨 {sym}: NAKED position (no stop, no take).")
                if trade.stop_price:
                    logger.warning(f"🔧 {sym}: attempting to re-place stop+take from DB data...")
                    replaced = await _try_reprotect_position(sym, trade)
                    if replaced:
                        logger.info(f"✅ {sym}: re-protected successfully")
                        _prot_set(sym, "protected")
                        await _send_tg_message(tg_channel,
                            f"🔧 <b>{sym}</b>: naked position detected at startup\n"
                            f"Stop и take <b>перевыставлены автоматически</b> ✅"
                        )
                    else:
                        logger.critical(f"🚨 {sym}: re-protect failed, force-closing")
                        await _force_close_position(sym, amt, "naked position: re-protect failed", binance_client)
                else:
                    await _force_close_position(sym, amt, "naked position: no stop price in DB", binance_client)

            # Position has take but no stop → try re-protect, fallback to close
            elif not has_stop:
                logger.critical(f"🚨 {sym}: no stop order (has take).")
                if trade.stop_price:
                    logger.warning(f"🔧 {sym}: attempting to re-place stop from DB data...")
                    replaced = await _try_reprotect_position(sym, trade)
                    if replaced:
                        logger.info(f"✅ {sym}: stop re-placed successfully")
                        _prot_set(sym, "protected")
                        await _send_tg_message(tg_channel,
                            f"🔧 <b>{sym}</b>: stop пропал, <b>перевыставлен автоматически</b> ✅"
                        )
                    else:
                        logger.critical(f"🚨 {sym}: re-place stop failed, force-closing")
                        await _force_close_position(sym, amt, "missing stop: re-protect failed", binance_client)
                else:
                    await _force_close_position(sym, amt, "missing stop order (no stop price in DB)", binance_client)

            # Position has stop but no take → check if normal (TP1 triggered) or missing
            elif not has_take:
                if not trade.take1_triggered:
                    logger.critical(f"🚨 {sym}: missing take order (TP1 not yet triggered).")
                    if trade.take1_price:
                        logger.warning(f"🔧 {sym}: attempting to re-place take order...")
                        replaced = await _try_reprotect_position(sym, trade)
                        if replaced:
                            logger.info(f"✅ {sym}: take re-placed successfully")
                            await _send_tg_message(tg_channel,
                                f"🔧 <b>{sym}</b>: take-profit пропал, <b>перевыставлен автоматически</b> ✅"
                            )
                        else:
                            logger.critical(f"🚨 {sym}: re-place take failed, force-closing")
                            await _force_close_position(sym, amt, "missing take: re-protect failed", binance_client)
                    else:
                        await _force_close_position(sym, amt, "missing take order (no take price in DB)", binance_client)
                else:
                    logger.info(f"ℹ️ {sym}: no take order — normal state after TP1 (stop at breakeven)")

        # ── 3. Stale DB trades (DB open, exchange flat) → close in DB ─────
        #    Also cancel any orphan orders that belong to stale trades
        #    (section 1 only cancels orders for symbols NOT in active_syms)
        stale = 0
        stale_symbols: set[str] = set()
        for sym, trade in db_by_sym.items():
            if sym not in binance_open_pos:
                stale_symbols.add(sym)
                logger.warning(
                    f"🔧 Stale DB trade: {sym} — position closed on exchange. "
                    f"Closing in DB."
                )
                # Cancel all standard orders for this symbol (belt and suspenders)
                try:
                    await binance_client.cancel_open_orders(symbol=sym)
                    logger.info(f"🔧 {sym}: cancelled all standard orders (stale trade cleanup)")
                except Exception as e:
                    if "-2011" not in str(e):  # -2011 = no orders to cancel
                        logger.warning(f"{sym}: cancel_open_orders in stale cleanup: {e}")
                # Cancel algo orders for this symbol
                for ao in exchange_algo_by_sym.get(sym, []):
                    try:
                        await binance_client.cancel_algo_order(algoId=ao["algoId"])
                        logger.info(f"🔧 {sym}: cancelled orphan algo {ao['algoId']} (stale trade cleanup)")
                    except Exception as e:
                        logger.warning(f"{sym}: cancel algo {ao.get('algoId')} failed: {e}")

                # Try to recover real PnL from Binance income history.
                # When bot was offline, WS fill events were missed → orders table still
                # shows realized_profit=0 → update_trade_result would return 0.
                # get_income_history returns the actual exchange-confirmed PnL.
                real_pnl: float | None = None
                try:
                    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    start_ms = trade.open_time or (now_ms - 7 * 24 * 3600 * 1000)
                    income_raw = await binance_client.get_income_history(
                        symbol=sym,
                        incomeType="REALIZED_PNL",
                        startTime=start_ms,
                        limit=50,
                    )
                    if isinstance(income_raw, list) and income_raw:
                        real_pnl = sum(
                            float(r.get("income", 0)) for r in income_raw
                            if r.get("time", 0) >= start_ms
                        ) or None
                    if real_pnl is not None:
                        await db.update_trade(trade.id, result=real_pnl)
                        logger.info(f"🔧 {sym}: recovered offline PnL = {real_pnl:+.4f} USDT")
                except Exception as ie:
                    logger.warning(f"{sym}: income history fetch failed: {ie}")

                await db.close_trade(trade.id, "CLOSED_MARKET", "RECONCILED")
                await db.update_trade_result(trade.id)
                stale += 1

                # Clean up in-memory state — must NOT have ghost entries
                _active_trades.pop(sym, None)
                _prot_clear(sym)
                asyncio.create_task(
                    _unsubscribe_price_stream(sym), name=f"unsub_stale_{sym}"
                )

                reconcile_pnl = real_pnl if real_pnl is not None else (trade.result or 0.0)
                await _send_tg_message(tg_channel,
                    f"🔧 <b>Startup cleanup</b>\n"
                    f"{'LONG' if trade.side else 'SHORT'} <b>{sym}</b>: "
                    f"закрыт (position already flat on exchange)\n"
                    f"PnL: <b>{reconcile_pnl:+.4f} USDT</b>"
                )

        # ── 4. Seed _active_trades cache ONLY for truly open positions ─────
        #    Done here (after stale cleanup) to avoid ghost entries for
        #    positions closed while the bot was offline.
        truly_open = [
            t for t in db_trades
            if t.symbol not in stale_symbols and t.symbol in binance_open_pos
        ]
        for t in truly_open:
            _active_trades[t.symbol] = t
        if truly_open:
            logger.info(f"📡 Restored {len(truly_open)} active trades to WS cache")
            for t in truly_open:
                asyncio.create_task(
                    _subscribe_price_stream(t.symbol), name=f"sub_startup_{t.symbol}"
                )

        # ── Send all alerts & summary ─────────────────────────────────────
        for msg in alerts:
            await notify_error(msg)

        if cancelled > 0 or stale > 0 or alerts:
            summary = (
                f"🔍 <b>Startup reconciliation</b>\n"
                f"Cancelled orphan orders: <b>{cancelled}</b>\n"
                f"Stale DB records closed: <b>{stale}</b>\n"
                f"Alerts sent: <b>{len(alerts)}</b>"
            )
            await _send_tg_message(tg_channel, summary)

        logger.info(
            f"✅ Startup reconciliation done: "
            f"cancelled_orders={cancelled} "
            f"alerts={len(alerts)} "
            f"stale_db={stale}"
        )

    except Exception as e:
        logger.error(f"Startup reconciliation failed: {e}\n{traceback.format_exc()}")


async def position_reconciliation():
    """Reconcile DB trades with actual Binance positions every 60s.
    Also handles manual /close requests from Telegram.
    """
    while not shutdown_event.is_set():
        if await _shutdown_sleep(60):
            break
        try:
            # 1. Handle /close requests
            db_trades = await db.get_all_open_trades()
            for trade in db_trades:
                close_req = await db.get_state(f"close_request_{trade.symbol}")
                if close_req == "1":
                    logger.info(f"📤 Manual close request: {trade.symbol}")
                    close_side = "SELL" if trade.side else "BUY"
                    try:
                        # Cancel all standard protective orders
                        try:
                            await client.cancel_open_orders(symbol=trade.symbol)
                        except Exception:
                            pass
                        # Cancel algo protective orders (testnet stop-limit)
                        try:
                            algo_raw = await client.get_algo_orders()
                            all_algo = (
                                algo_raw.get("algoOrders", [])
                                if isinstance(algo_raw, dict)
                                else (algo_raw if isinstance(algo_raw, list) else [])
                            )
                            for ao in all_algo:
                                if ao.get("symbol") == trade.symbol:
                                    try:
                                        await client.cancel_algo_order(algoId=ao["algoId"])
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        # Market close
                        pos = await client.get_position_risk(symbol=trade.symbol)
                        pos_info = next((p for p in pos if p["symbol"] == trade.symbol), None)
                        qty = abs(float(pos_info["positionAmt"])) if pos_info else trade.quantity
                        if qty > 0:
                            # BUG-2 fix: format as string with correct precision (no scientific notation)
                            sym_info = all_symbols.get(trade.symbol)
                            step = sym_info.step_size if sym_info else 3
                            qty_str = f"{qty:.{max(0, int(step))}f}"
                            await client.new_order(
                                symbol=trade.symbol, side=close_side,
                                type="MARKET", quantity=qty_str, reduceOnly="true",
                            )
                            await db.close_trade(trade.id, "CLOSED_MARKET", "MANUAL")
                            await db.update_trade_result(trade.id)
                            positions[trade.symbol] = False
                            await _send_tg_message(tg_channel,
                                f"✅ <b>{trade.symbol}</b> closed manually.")
                        else:
                            # Binance already flat — just clean up DB
                            logger.warning(f"{trade.symbol}: qty=0 on Binance, closing DB only")
                            await db.close_trade(trade.id, "CLOSED_MARKET", "MANUAL")
                            await db.update_trade_result(trade.id)
                            positions[trade.symbol] = False
                            await _send_tg_message(tg_channel,
                                f"✅ <b>{trade.symbol}</b> already flat, DB closed.")
                    except Exception as e:
                        logger.error(f"Manual close {trade.symbol} failed: {e}")
                        await _send_tg_message(tg_channel,
                            f"❌ Failed to close <b>{trade.symbol}</b>: {e}")
                    await db.set_state(f"close_request_{trade.symbol}", "0")

            # 2. Reconcile: DB open trades vs Binance positions
            db_trades = await db.get_all_open_trades()
            try:
                binance_positions = await client.get_position_risk()
                binance_open = {
                    p["symbol"]: float(p["positionAmt"])
                    for p in binance_positions
                    if abs(float(p["positionAmt"])) > 0 and "_" not in p["symbol"]
                }
            except Exception as e:
                logger.warning(f"Reconciliation: position fetch failed: {e}")
                continue

            db_symbols = {t.symbol for t in db_trades}

            # DB says open, Binance says closed → close in DB + cancel orphan orders
            for trade in db_trades:
                if trade.symbol not in binance_open:
                    logger.warning(f"🔧 Reconciliation: {trade.symbol} closed on exchange but open in DB")
                    # Cancel all standard orders for this symbol first
                    try:
                        await client.cancel_open_orders(symbol=trade.symbol)
                        logger.info(f"🔧 {trade.symbol}: cancelled standard orders (reconciliation)")
                    except Exception as e:
                        if "-2011" not in str(e):  # -2011 = no orders
                            logger.warning(f"{trade.symbol}: cancel_open_orders: {e}")
                    # Cancel algo orders for this symbol
                    try:
                        algo_raw = await client.get_algo_orders()
                        all_algo = (
                            algo_raw.get("algoOrders", [])
                            if isinstance(algo_raw, dict)
                            else (algo_raw if isinstance(algo_raw, list) else [])
                        )
                        for ao in all_algo:
                            if ao.get("symbol") == trade.symbol:
                                try:
                                    await client.cancel_algo_order(algoId=ao["algoId"])
                                    logger.info(f"🔧 {trade.symbol}: cancelled algo {ao['algoId']} (reconciliation)")
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    # Try to recover real PnL from Binance income history.
                    # WS fill events may have been missed (bot was offline or WS dropped).
                    real_pnl: float | None = None
                    try:
                        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                        start_ms = trade.open_time or (now_ms - 7 * 24 * 3600 * 1000)
                        income_raw = await client.get_income_history(
                            symbol=trade.symbol,
                            incomeType="REALIZED_PNL",
                            startTime=start_ms,
                            limit=50,
                        )
                        if isinstance(income_raw, list) and income_raw:
                            real_pnl = sum(
                                float(r.get("income", 0)) for r in income_raw
                                if r.get("time", 0) >= start_ms
                            ) or None
                        if real_pnl is not None:
                            await db.update_trade(trade.id, result=real_pnl)
                            logger.info(f"🔧 {trade.symbol}: recovered PnL = {real_pnl:+.4f}")
                    except Exception as ie:
                        logger.warning(f"{trade.symbol}: income history fetch failed: {ie}")

                    await db.close_trade(trade.id, "CLOSED_MARKET", "RECONCILED")
                    await db.update_trade_result(trade.id)
                    positions[trade.symbol] = False

                    # Clean up in-memory state
                    _active_trades.pop(trade.symbol, None)
                    _prot_clear(trade.symbol)
                    asyncio.create_task(
                        _unsubscribe_price_stream(trade.symbol),
                        name=f"unsub_reconcile_{trade.symbol}",
                    )

                    reconcile_pnl = real_pnl if real_pnl is not None else (trade.result or 0.0)
                    await _send_tg_message(
                        tg_channel,
                        f"🔧 <b>{'LONG' if trade.side else 'SHORT'} {trade.symbol}</b> "
                        f"закрыт (reconciliation)\n"
                        f"Позиция уже была закрыта на бирже\n"
                        f"PnL: <b>{reconcile_pnl:+.4f} USDT</b>"
                    )
                else:
                    # DB trade is open AND exchange position exists -> verify side matches
                    amt = binance_open[trade.symbol]
                    db_side_is_long = bool(trade.side)
                    exchange_side_is_long = (amt > 0)
                    if db_side_is_long != exchange_side_is_long:
                        logger.critical(
                            f"🚨 Reconciliation side mismatch on {trade.symbol}! DB={'LONG' if db_side_is_long else 'SHORT'}, "
                            f"Exchange={'LONG' if exchange_side_is_long else 'SHORT'} ({amt}). Force-closing."
                        )
                        mismatch_msg = (
                            f"🚨 <b>Reconciliation Side Mismatch on {trade.symbol}!</b>\n"
                            f"Биржа: <b>{'LONG' if exchange_side_is_long else 'SHORT'}</b> ({amt})\n"
                            f"БД: <b>{'LONG' if db_side_is_long else 'SHORT'}</b> ({trade.quantity})\n"
                            f"Аварийно закрываем позицию на бирже."
                        )
                        await _send_tg_message(tg_channel, mismatch_msg)
                        await notify_error(mismatch_msg)
                        
                        # Force-close on exchange
                        await _force_close_position(trade.symbol, amt, f"reconciliation side mismatch with DB trade #{trade.id}", client)
                        
                        # Close trade in DB
                        await db.close_trade(trade.id, "CLOSED_MARKET", "RECONCILED")
                        await db.update_trade_result(trade.id)
                        positions[trade.symbol] = False
                        
                        # Clean up in-memory state
                        _active_trades.pop(trade.symbol, None)
                        _prot_clear(trade.symbol)
                        asyncio.create_task(
                            _unsubscribe_price_stream(trade.symbol),
                            name=f"unsub_reconcile_mismatch_{trade.symbol}",
                        )


            # Binance has position, DB doesn't know → alert
            for sym in binance_open:
                if sym not in db_symbols:
                    logger.critical(f"🚨 ORPHAN position: {sym} ({binance_open[sym]}) — not in DB!")
                    await notify_error(f"Orphan position detected: {sym} qty={binance_open[sym]}")

            # 3. Verify stop protection via in-memory cache (NO REST polling).
            #    REST is called ONLY when cache says "unprotected" — as safety net
            #    before any destructive action.  This prevents the false-positive
            #    auto-closes caused by transient API failures.
            for sym, amt in binance_open.items():
                if _prot_is_protected(sym):
                    logger.debug(f"✅ {sym}: cache=protected (skip REST check)")
                    continue

                # Cache says unprotected or unknown — REST-verify before force-close
                state = _protection_state.get(sym, "unknown")
                logger.warning(
                    f"⚠️ {sym}: cache={state}, REST-verifying before any action..."
                )
                if await _rest_verify_no_protection(sym, client):
                    # Confirmed unprotected — try to RE-PLACE orders before force-close
                    trade = _active_trades.get(sym)
                    if trade is None:
                        trade = await db.get_open_trade(sym)

                    if trade and trade.stop_price:
                        logger.warning(
                            f"🔧 {sym}: CONFIRMED no protection, attempting to re-place stop+take..."
                        )
                        replaced = await _try_reprotect_position(sym, trade)
                        if replaced:
                            logger.info(f"✅ {sym}: stop+take re-placed successfully")
                            _prot_set(sym, "protected")
                            await _send_tg_message(
                                tg_channel,
                                f"🔧 <b>{sym}</b>: обнаружена позиция без защиты\n"
                                f"Стоп и тейк <b>перевыставлены автоматически</b> ✅"
                            )
                            continue
                        logger.critical(f"🚨 {sym}: re-protect failed, force-closing")

                    logger.critical(f"🚨 {sym}: CONFIRMED no stop protection. Auto-closing.")
                    await _force_close_position(
                        sym, amt,
                        "missing stop order confirmed by REST verification",
                        client,
                    )
                # else: REST found stop or couldn't verify → skip (safe default)

            # 4. Orphan orders on exchange (order exists, position flat) —
            #    cancel standard + algo orders for stale DB symbols (no per-order REST)
            db_syms_open = {t.symbol for t in db_trades}
            for sym in list(db_syms_open):
                if sym not in binance_open:
                    try:
                        await client.cancel_open_orders(symbol=sym)
                        logger.info(f"🔧 Reconcile: cancelled all orders on {sym} (position flat)")
                    except Exception as e:
                        if "-2011" not in str(e):
                            logger.warning(f"Cancel orphan orders on {sym}: {e}")
                    # Also cancel algo orders
                    try:
                        algo_raw = await client.get_algo_orders(symbol=sym)
                        algo_list = (
                            algo_raw.get("algoOrders", []) if isinstance(algo_raw, dict)
                            else (algo_raw if isinstance(algo_raw, list) else [])
                        )
                        for ao in algo_list:
                            if ao.get("symbol") == sym:
                                try:
                                    await client.cancel_algo_order(algoId=ao["algoId"])
                                    logger.info(f"🔧 Reconcile: cancelled orphan algo {ao['algoId']} on {sym}")
                                except Exception:
                                    pass
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"Reconciliation error: {e}\n{traceback.format_exc()}")


async def message_sender():
    """Send TG messages with flood control. Shutdown-aware (BUG-7)."""
    while not shutdown_event.is_set():
        try:
            item = await asyncio.wait_for(message_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            continue
        # Support both 3-tuple (legacy) and 4-tuple (with reply_to_message_id)
        if len(item) == 4:
            chat_id, text, future, reply_to = item
        else:
            chat_id, text, future = item
            reply_to = None
        try:
            if chat_id and hasattr(tg, "bot") and tg.bot:
                while True:
                    try:
                        kwargs = dict(chat_id=chat_id, text=text, parse_mode="HTML")
                        if reply_to:
                            kwargs["reply_to_message_id"] = reply_to
                        msg = await tg.bot.send_message(**kwargs)
                        future.set_result(msg.message_id)
                        await asyncio.sleep(1.2)
                        break
                    except Exception as e:
                        err_str = str(e).lower()
                        if "retry after" in err_str:
                            import re
                            match = re.search(r"retry after (\d+)", str(e), re.IGNORECASE)
                            delay = int(match.group(1)) + 1 if match else 5
                            await asyncio.sleep(delay)
                            continue
                        # reply_to_message_id points to deleted/inaccessible message
                        if reply_to and "reply message not found" in err_str:
                            logger.warning("TG: reply target not found, resending without thread")
                            reply_to = None
                            continue
                        # Non-retryable error — break inner loop, handle below
                        raise
            else:
                future.set_result(None)
        except Exception as e:
            logger.error(f"TG send error: {e}")
            if not future.done():
                future.set_result(None)
        finally:
            # Always mark task done to prevent queue.join() deadlock
            message_queue.task_done()


async def error_sender():
    """Send error messages to error channel. Shutdown-aware (BUG-7)."""
    while not shutdown_event.is_set():
        try:
            chat_id, text, future = await asyncio.wait_for(
                error_queue.get(), timeout=5.0
            )
        except asyncio.TimeoutError:
            continue
        try:
            if chat_id and hasattr(tg, "bot") and tg.bot:
                msg = await tg.bot.send_message(chat_id, text, parse_mode="HTML")
                future.set_result(msg.message_id)
            else:
                future.set_result(None)
        except Exception as e:
            logger.error(f"Error send failed: {e}")
            if not future.done():
                future.set_result(None)
        finally:
            # BUG-1 fix: task_done() MUST be in finally to prevent queue deadlock
            await asyncio.sleep(1.5)
            error_queue.task_done()


async def notify_error(message: str, key: str = None):
    """Send critical error with anti-spam."""
    import time as _time
    now = _time.time()
    key = key or message
    if now - error_send_cache[key] >= ERROR_SEND_INTERVAL:
        error_send_cache[key] = now
        text = f"🚨 <b>Критическая ошибка</b>\n\n<code>{message[:500]}</code>"
        future = asyncio.get_running_loop().create_future()
        await error_queue.put((error_channel, text, future))


# ============================================================
# Entry Point
# ============================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        shutdown_event.set()
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}\n{traceback.format_exc()}")
    finally:
        release_lock()
