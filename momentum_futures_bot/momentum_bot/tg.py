"""Telegram bot interface for Momentum Trading Bot v4.4.

Admin-only FSM UI. All exchange events arrive via main.py → message_queue.
This module only handles commands/callbacks and DB reads/writes.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime

from aiogram import Bot, Dispatcher, F, BaseMiddleware, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    TelegramObject,
)

import db

logger = logging.getLogger(__name__)

bot: Bot = None
dp = Dispatcher()
ADMINS: list[int] = []
_client = None  # Binance client reference — injected by main.py

# Кнопки главного меню — используется в middleware для определения "навигационных" сообщений
MAIN_NAV_BUTTONS = {"📊 Positions", "📈 Statistics", "🌐 Symbols", "⚙️ Settings", "🔍 Status"}


# ============================================================
# Admin-only Middleware (runs before every handler)
# ============================================================
class AdminMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        user = data.get("event_from_user")
        if user is None or user.id not in ADMINS:
            if isinstance(event, CallbackQuery):
                await event.answer("⛔ Access denied", show_alert=True)
            elif isinstance(event, Message):
                await event.answer(
                    f"⛔ Access denied.\nYour ID: <code>{user.id if user else '?'}</code>",
                    parse_mode="HTML",
                )
            return
        return await handler(event, data)


# ============================================================
# Navigation Middleware — clears FSM when main menu is pressed
# Must run AFTER AdminMiddleware so only admins reach here.
# ============================================================
class NavMiddleware(BaseMiddleware):
    """Intercepts main keyboard button presses in any FSM state and resets state.

    In aiogram 3.x, FSM-filtered handlers take priority over plain handlers.
    This middleware runs before routing, clearing the state so the plain
    handlers (cmd_positions, cmd_settings, etc.) will match instead.
    """
    async def __call__(self, handler, event: TelegramObject, data: dict):
        if isinstance(event, Message) and event.text in MAIN_NAV_BUTTONS:
            state: FSMContext = data.get("state")
            if state:
                current = await state.get_state()
                if current is not None:
                    await state.clear()
        return await handler(event, data)


# ============================================================
# FSM States
# ============================================================
class States(StatesGroup):
    settings           = State()
    # API
    change_keys        = State()
    # Risk / position params
    change_leverage    = State()
    change_risk        = State()
    change_max_pos     = State()
    change_entries_bar = State()
    change_cooldown    = State()
    # Exit / ATR params
    change_stop_atr    = State()
    change_take1_atr   = State()
    change_take2_atr   = State()
    change_partial_pct = State()
    # Signal params
    change_interval    = State()
    change_signal_mode = State()
    change_trend_min   = State()
    change_mom_min     = State()
    change_timing_min  = State()
    change_comp_min    = State()
    change_max_pos_usdt = State()
    # Misc
    symbols            = State()
    close_pos          = State()


# ============================================================
# Keyboards
# ============================================================
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📊 Positions"),  KeyboardButton(text="📈 Statistics")],
        [KeyboardButton(text="🌐 Symbols"),    KeyboardButton(text="⚙️ Settings")],
        [KeyboardButton(text="🔍 Status")],
    ], resize_keyboard=True)


def kb_positions(has_trades: bool) -> InlineKeyboardMarkup:
    rows = []
    if has_trades:
        rows.append([InlineKeyboardButton(text="📤 Close position", callback_data="close_pos")])
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="positions")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_settings(conf: db.ConfigInfo) -> InlineKeyboardMarkup:
    trade_label   = "⏸ Disable trading" if conf.trade_mode else "▶️ Enable trading"
    testnet_label = f"🌐 Testnet: {'ON→OFF' if conf.testnet else 'OFF→ON'}"
    lev  = int(conf.leverage or 20)
    risk = float(conf.risk_per_trade or 0.01) * 100
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=trade_label,                       callback_data="toggle_trade")],
        [InlineKeyboardButton(text="🔑 Change API keys",               callback_data="change_keys")],
        [InlineKeyboardButton(text=testnet_label,                     callback_data="toggle_testnet")],
        [InlineKeyboardButton(text=f"⚖️ Leverage: {lev}x",             callback_data="change_leverage"),
         InlineKeyboardButton(text=f"🎯 Risk: {risk:.1f}%",             callback_data="change_risk")],
        [InlineKeyboardButton(text="📦 Position limits ▸",              callback_data="submenu_position")],
        [InlineKeyboardButton(text="🚪 Exit / ATR params ▸",            callback_data="submenu_exit")],
        [InlineKeyboardButton(text="📊 Signal thresholds ▸",           callback_data="submenu_signal")],
        [InlineKeyboardButton(text="🔄 Restart bot",                    callback_data="restart_bot")],
    ])


def kb_submenu_position(conf: db.ConfigInfo) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📊 Max positions: {conf.max_positions or 5}",       callback_data="change_max_pos"),
         InlineKeyboardButton(text=f"📊 Entries/bar: {conf.max_entries_per_bar or 2}",    callback_data="change_entries_bar")],
        [InlineKeyboardButton(text=f"⏳ Cooldown bars: {conf.symbol_cooldown or 3}",     callback_data="change_cooldown")],
        [InlineKeyboardButton(text=f"📆 Interval: {conf.interval or '4h'}",           callback_data="change_interval"),
         InlineKeyboardButton(text=f"🔍 Mode: {conf.signal_mode or 'structured'}",    callback_data="change_signal_mode")],
        [InlineKeyboardButton(text="← Back",                                            callback_data="back_settings")],
    ])


def kb_submenu_exit(conf: db.ConfigInfo) -> InlineKeyboardMarkup:
    adaptive = "✅ ON" if conf.use_adaptive_sizing else "❌ OFF"
    usdt_cap = float(conf.manual_equity or 0)
    cap_label = f"${usdt_cap:.0f}" if usdt_cap > 0 else "авто"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🛑 Stop ATR: {conf.stop_atr or 1.5}",            callback_data="change_stop_atr"),
         InlineKeyboardButton(text=f"🏹 TP1 ATR: {conf.take1_atr or 2.0}",           callback_data="change_take1_atr")],
        [InlineKeyboardButton(text=f"🎯 TP2 ATR: {conf.take2_atr or 4.0}",            callback_data="change_take2_atr"),
         InlineKeyboardButton(text=f"✋ Partial exit: {float(conf.partial_exit_pct or 0.1)*100:.0f}%", callback_data="change_partial_pct")],
        [InlineKeyboardButton(text=f"📌 Adaptive sizing: {adaptive}",               callback_data="toggle_adaptive")],
        [InlineKeyboardButton(text=f"💰 Equity (sim): {cap_label}",                  callback_data="change_max_pos_usdt")],
        [InlineKeyboardButton(text="← Back",                                           callback_data="back_settings")],
    ])


def kb_submenu_signal(conf: db.ConfigInfo) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📈 Trend min: {conf.struct_trend_min or 0.5}",    callback_data="change_trend_min"),
         InlineKeyboardButton(text=f"⚡ Momentum min: {conf.struct_momentum_min or 0.4}", callback_data="change_mom_min")],
        [InlineKeyboardButton(text=f"⏲ Timing min: {conf.struct_timing_min or 0.25}",   callback_data="change_timing_min"),
         InlineKeyboardButton(text=f"🧠 Composite min: {conf.struct_composite_min or 0.25}", callback_data="change_comp_min")],
        [InlineKeyboardButton(text="← Back",                                            callback_data="back_settings")],
    ])


def kb_symbols() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add symbol",    callback_data="sym_add"),
         InlineKeyboardButton(text="➖ Remove symbol", callback_data="sym_del")],
        [InlineKeyboardButton(text="🔄 Reset to default", callback_data="sym_reset")],
    ])


# ============================================================
# Universal reply helper
# ============================================================
async def reply(event: Message | CallbackQuery, text: str,
                reply_markup=None, parse_mode: str = "HTML"):
    """Send answer for both Message and CallbackQuery."""
    if isinstance(event, CallbackQuery):
        await event.answer()
        await event.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
    else:
        await event.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)


# ============================================================
# /start  /help  /menu
# ============================================================
@dp.message(Command("start", "help", "menu"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "🤖 <b>Momentum Bot v4.4</b>\n\n"
        "Use the buttons below to manage the bot.",
        parse_mode="HTML",
        reply_markup=kb_main(),
    )


# ============================================================
# Status — live snapshot from DB
# ============================================================
@dp.message(F.text == "🔍 Status")
@dp.message(Command("status"))
async def cmd_status(message: Message, state: FSMContext):
    await state.clear()
    conf = await db.load_config()
    trades = await db.get_all_open_trades()
    total_pnl = await db.get_total_realized_pnl()

    mode_str = "✅ ON"  if conf.trade_mode else "⏸ OFF"
    net_str  = "Testnet" if conf.testnet   else "Mainnet"
    keys_str = "✅ Set"  if conf.has_keys  else "❌ Missing"

    text = (
        f"🔍 <b>Bot Status</b>\n\n"
        f"Trading:  <b>{mode_str}</b>\n"
        f"Network:  <b>{net_str}</b>\n"
        f"API Keys: <b>{keys_str}</b>\n\n"
        f"💰 Realized PnL: <b>{total_pnl:+.2f} USDT</b>\n"
        f"📊 Open trades:  <b>{len(trades)}</b>\n"
    )

    if trades:
        text += "\n<b>Open positions:</b>\n"
        for t in trades:
            direction = "🟢 LONG" if t.side else "🔴 SHORT"
            tp1_tag = "✅" if t.take1_triggered else "⏳"
            text += (
                f"  • <b>{t.symbol}</b> {direction}\n"
                f"    Entry {t.entry_price:.4f} | Stop {t.stop_price:.4f}\n"
                f"    TP1 {t.take1_price:.4f} {tp1_tag} | TP2 {t.take2_price:.4f}\n"
            )

    await message.answer(text, parse_mode="HTML", reply_markup=kb_main())


# ============================================================
# Positions
# ============================================================
@dp.message(F.text == "📊 Positions")
@dp.callback_query(F.data == "positions")
async def cmd_positions(event: Message | CallbackQuery, state: FSMContext):
    await state.clear()
    trades = await db.get_all_open_trades()

    if not trades:
        await reply(event, "📭 No open positions.", reply_markup=kb_main())
        return

    # Fetch real-time position risk for live PnL from Binance
    real_pnls = {}
    if _client is not None:
        try:
            pos_risk = await _client.get_position_risk()
            for p in pos_risk:
                amt = float(p.get("positionAmt", 0))
                if abs(amt) > 0:
                    real_pnls[p["symbol"]] = float(p.get("unRealizedProfit", 0))
        except Exception as e:
            logger.warning(f"⚠️ Cannot fetch position risk for Telegram display: {e}")

    text = f"📊 <b>Open positions ({len(trades)})</b>\n"
    for t in trades:
        direction = "🟢 LONG" if t.side else "🔴 SHORT"
        tp1_tag   = "✅" if t.take1_triggered else "⏳"
        
        # Use live exchange PnL if available, else fall back to DB result
        if t.symbol in real_pnls:
            pnl_val = real_pnls[t.symbol]
            pnl_str = f"{pnl_val:+.4f} USDT"
        else:
            pnl_str = f"{t.result:+.4f} USDT" if t.result else "—"

        text += (
            f"\n<b>{t.symbol}</b> {direction}\n"
            f"  Entry: {t.entry_price:.4f} | Stop: {t.stop_price:.4f}\n"
            f"  TP1: {t.take1_price:.4f} {tp1_tag} | TP2: {t.take2_price:.4f}\n"
            f"  Score: {t.signal_score:.3f} | Mult: {t.risk_multiplier:.2f}x\n"
            f"  PnL: <b>{pnl_str}</b>\n"
        )

    await reply(event, text, reply_markup=kb_positions(has_trades=True))


@dp.callback_query(F.data == "close_pos")
async def close_pos_select(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.close_pos)
    trades = await db.get_all_open_trades()
    if not trades:
        await reply(callback, "📭 No open positions to close.")
        return

    rows = [
        [InlineKeyboardButton(
            text=f"{'🟢' if t.side else '🔴'} {t.symbol} | entry {t.entry_price:.4f}",
            callback_data=f"close:{t.symbol}",
        )]
        for t in trades
    ]
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="positions")])
    await reply(callback, "Select position to close:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@dp.callback_query(States.close_pos, F.data.startswith("close:"))
async def close_pos_confirm(callback: CallbackQuery, state: FSMContext):
    symbol = callback.data.split(":")[1]
    await db.set_state(f"close_request_{symbol}", "1")
    await state.clear()
    await reply(
        callback,
        f"⏳ Close request sent for <b>{symbol}</b>.\n"
        f"Position will be closed within 60 seconds.",
    )


# ============================================================
# Statistics
# ============================================================
@dp.message(F.text == "📈 Statistics")
@dp.message(Command("stats"))
async def cmd_stats(message: Message, state: FSMContext):
    await state.clear()
    stats = await db.get_trade_stats()

    if stats["total"] == 0:
        await message.answer("📭 No closed trades yet.", reply_markup=kb_main())
        return

    text = (
        f"📈 <b>Performance Summary</b>\n\n"
        f"Total trades:  <b>{stats['total']}</b>\n"
        f"Win / Loss:    <b>{stats['wins']} / {stats['losses']}</b> ({stats['wr']}%)\n"
        f"Profit Factor: <b>{stats['pf']}</b>\n"
        f"Total PnL:     <b>{stats['pnl']:+.2f} USDT</b>\n"
        f"Avg PnL:       <b>{stats['avg_pnl']:+.4f} USDT</b>\n"
        f"Best / Worst:  <b>{stats['best']:+.2f} / {stats['worst']:+.2f}</b>\n"
    )

    logs = await db.get_equity_logs(limit=12)
    if logs:
        text += "\n💰 <b>Equity log (last 12h):</b>\n"
        for log in logs:
            ts = log.timestamp.strftime("%m/%d %H:%M") if log.timestamp else "—"
            upnl = f" | uPnL {log.unrealized_pnl:+.2f}" if log.unrealized_pnl else ""
            text += f"  {ts}  <b>${log.equity:,.2f}</b> ({log.open_positions} pos{upnl})\n"

    await message.answer(text, parse_mode="HTML", reply_markup=kb_main())


# ============================================================
# Symbols
# ============================================================
@dp.message(F.text == "🌐 Symbols")
@dp.callback_query(F.data == "symbols_list")
async def cmd_symbols(event: Message | CallbackQuery, state: FSMContext):
    await state.set_state(States.symbols)
    symbols = await db.get_active_symbols()

    text = f"🌐 <b>Active symbols ({len(symbols)})</b>\n\n"
    for i, s in enumerate(symbols, 1):
        text += f"{i:2d}. {s}\n"

    await reply(event, text, reply_markup=kb_symbols())


@dp.callback_query(States.symbols, F.data == "sym_add")
async def sym_add_prompt(callback: CallbackQuery, state: FSMContext):
    await state.update_data(sym_action="add")
    await reply(callback, "Enter ticker to <b>add</b> (e.g. ETHUSDT):")


@dp.callback_query(States.symbols, F.data == "sym_del")
async def sym_del_prompt(callback: CallbackQuery, state: FSMContext):
    await state.update_data(sym_action="del")
    await reply(callback, "Enter ticker to <b>remove</b>:")


@dp.callback_query(States.symbols, F.data == "sym_reset")
async def sym_reset(callback: CallbackQuery, state: FSMContext):
    await db.reset_symbols()
    symbols = await db.get_active_symbols()
    await reply(callback, f"✅ Universe reset to default ({len(symbols)} symbols).")
    await cmd_symbols(callback, state)


@dp.message(States.symbols)
async def sym_action_handler(message: Message, state: FSMContext):
    # NavMiddleware already cleared state if this is a main nav button,
    # so by the time we get here the text is a real ticker input.
    data = await state.get_data()
    action = data.get("sym_action", "add")
    symbol = message.text.strip().upper()

    if not symbol.endswith("USDT"):
        await message.answer("❌ Ticker must end with USDT (e.g. SOLUSDT).")
        return

    if action == "add":
        await db.add_symbol(symbol)
        await message.answer(f"✅ <b>{symbol}</b> added to universe.")
    else:
        removed = await db.remove_symbol(symbol)
        if removed:
            await message.answer(f"✅ <b>{symbol}</b> removed from universe.")
        else:
            await message.answer(f"❌ <b>{symbol}</b> not found in universe.")

    await cmd_symbols(message, state)


# ============================================================
# Settings
# ============================================================
@dp.message(F.text == "⚙️ Settings")
@dp.callback_query(F.data == "settings")
async def cmd_settings(event: Message | CallbackQuery, state: FSMContext):
    await state.set_state(States.settings)
    conf = await db.load_config()

    api_display = (
        f"{conf.api_key[:4]}...{conf.api_key[-4:]}" if conf.api_key else "❌ Not set"
    )
    lev = int(conf.leverage or 20)
    risk_pct = float(conf.risk_per_trade or 0.01) * 100
    adaptive_str = "ON" if conf.use_adaptive_sizing else "OFF"
    text = (
        f"⚙️ <b>Settings</b>\n\n"
        f"<b>Connection</b>\n"
        f"Trading:  <b>{'✅ ON' if conf.trade_mode else '⏸ OFF'}</b>  "
        f"Network: <b>{'Testnet' if conf.testnet else '🔴 Mainnet'}</b>\n"
        f"API Key:  <b>{api_display}</b>\n\n"
        f"<b>Risk</b>\n"
        f"Leverage: <b>{lev}x</b>  Risk/trade: <b>{risk_pct:.1f}%</b>\n"
        f"Max pos: <b>{conf.max_positions or 5}</b>  "
        f"Entries/bar: <b>{conf.max_entries_per_bar or 2}</b>  "
        f"Cooldown: <b>{conf.symbol_cooldown or 3}b</b>\n\n"
        f"<b>Signal</b>\n"
        f"Interval: <b>{conf.interval or '4h'}</b>  "
        f"Mode: <b>{conf.signal_mode or 'structured'}</b>\n\n"
        f"<b>Exit</b>\n"
        f"Stop: <b>{conf.stop_atr or 1.5}×ATR</b>  "
        f"TP1: <b>{conf.take1_atr or 2.0}×</b>  "
        f"TP2: <b>{conf.take2_atr or 4.0}×</b>  "
        f"Partial: <b>{float(conf.partial_exit_pct or 0.1)*100:.0f}%</b>\n"
        f"Adaptive sizing: <b>{adaptive_str}</b> "
        f"({conf.adaptive_min_mult or 0.5}×–{conf.adaptive_max_mult or 1.5}×)\n\n"
        f"<b>Signal thresholds</b>\n"
        f"Trend: <b>{conf.struct_trend_min or 0.5}</b>  "
        f"Momentum: <b>{conf.struct_momentum_min or 0.4}</b>  "
        f"Timing: <b>{conf.struct_timing_min or 0.25}</b>  "
        f"Composite: <b>{conf.struct_composite_min or 0.25}</b>"
    )
    await reply(event, text, reply_markup=kb_settings(conf))


@dp.callback_query(States.settings, F.data == "toggle_trade")
async def toggle_trade(callback: CallbackQuery, state: FSMContext):
    conf = await db.load_config()
    if not conf.trade_mode and not conf.has_keys:
        await callback.answer("❌ Set API keys first.", show_alert=True)
        return
    new_mode = "0" if conf.trade_mode else "1"
    await db.config_update(trade_mode=new_mode)
    label = "enabled ✅" if new_mode == "1" else "disabled ⏸"
    await callback.answer(f"Trading {label}.")
    await cmd_settings(callback, state)


@dp.callback_query(States.settings, F.data == "toggle_testnet")
async def toggle_testnet(callback: CallbackQuery, state: FSMContext):
    conf = await db.load_config()
    new_val = "0" if conf.testnet else "1"
    await db.config_update(testnet=new_val)
    net = "Testnet" if new_val == "1" else "Mainnet"
    await callback.answer(f"Switched to {net}. ⚠️ Restart required.", show_alert=True)
    await cmd_settings(callback, state)


@dp.callback_query(States.settings, F.data == "restart_bot")
async def restart_bot(callback: CallbackQuery, state: FSMContext):
    """Restart the bot process. Systemd will auto-restart it."""
    await callback.answer("🔄 Перезапуск бота...")
    await callback.message.answer(
        "🔄 <b>Бот перезапускается...</b>\n"
        "Все позиции защищены стопами и тейками.\n"
        "Бот вернётся через ~5 секунд.",
        parse_mode="HTML",
    )
    logger.info("🔄 Restart requested via Telegram")
    await state.clear()
    # Give TG message time to send, then exit.
    # Systemd Restart=always will bring us back.
    await asyncio.sleep(1)
    os._exit(42)  # special exit — systemd restarts the service


# ============================================================
# Change API keys (2-step FSM)
# ============================================================
@dp.callback_query(States.settings, F.data == "change_keys")
async def change_keys_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_keys)
    await state.update_data(api_key=None)
    await callback.answer()
    await callback.message.answer("🔑 Enter your <b>API KEY</b>:")


@dp.message(States.change_keys)
async def change_keys_value(message: Message, state: FSMContext):
    data = await state.get_data()

    # Step 1: collect API key
    if not data.get("api_key"):
        await state.update_data(api_key=message.text.strip())
        try:
            await message.delete()
        except Exception:
            pass
        await message.answer("🔑 Enter your <b>SECRET KEY</b>:")
        return

    # Step 2: collect secret, save (skip live validation — no client yet)
    api_key    = data["api_key"]
    secret_key = message.text.strip()
    try:
        await message.delete()
    except Exception:
        pass

    await db.config_update(api_key=api_key, api_secret=secret_key)
    await message.answer(
        f"✅ API keys saved!\n"
        f"Key: <code>{api_key[:4]}...{api_key[-4:]}</code>\n\n"
        f"Use <b>⚙️ Settings → Enable trading</b> to start."
    )
    await state.set_state(States.settings)
    await cmd_settings(message, state)




# ============================================================
@dp.callback_query(States.settings, F.data == "change_leverage")
async def change_leverage_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_leverage)
    conf = await db.load_config()
    await callback.answer()
    await callback.message.answer(
        f"⚖️ Enter new <b>leverage</b> (1–125).\n"
        f"Current: <b>{int(conf.leverage or 20)}x</b>\n\n"
        f"⚠️ Applies to all new positions. Existing positions unaffected."
    )


@dp.message(States.change_leverage)
async def change_leverage_value(message: Message, state: FSMContext):
    try:
        value = int(message.text.strip())
        if not (1 <= value <= 125):
            raise ValueError
    except ValueError:
        await message.answer("❌ Enter integer between 1 and 125:")
        return
    await db.config_update(leverage=str(value))
    await message.answer(f"✅ Leverage set to <b>{value}x</b>.\nApplies to all new trades.")
    await state.set_state(States.settings)
    await cmd_settings(message, state)


# ============================================================
# Change risk per trade
# ============================================================
@dp.callback_query(States.settings, F.data == "change_risk")
async def change_risk_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_risk)
    conf = await db.load_config()
    current_pct = float(conf.risk_per_trade or 0.01) * 100
    await callback.answer()
    await callback.message.answer(
        f"🎯 Enter new <b>risk per trade</b> in % (0.1–5.0).\n"
        f"Current: <b>{current_pct:.1f}%</b>\n\n"
        f"Example: <code>1.0</code> = 1% of equity per trade.\n"
        f"Adaptive sizing still applies (0.5×–1.5× multiplier)."
    )


@dp.message(States.change_risk)
async def change_risk_value(message: Message, state: FSMContext):
    try:
        pct = float(message.text.strip().replace(",", ".").replace("%", ""))
        if not (0.1 <= pct <= 5.0):
            raise ValueError
    except ValueError:
        await message.answer("❌ Enter a number between 0.1 and 5.0 (e.g. <code>1.0</code>):")
        return
    fraction = pct / 100
    await db.config_update(risk_per_trade=str(fraction))
    await message.answer(
        f"✅ Risk per trade set to <b>{pct:.1f}%</b>.\n"
        f"Effective on next signal cycle (no restart needed)."
    )
    await state.set_state(States.settings)
    await cmd_settings(message, state)







# ============================================================
# Sub-menu navigation
# ============================================================
@dp.callback_query(States.settings, F.data == "submenu_position")
async def submenu_position_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conf = await db.load_config()
    await callback.message.edit_reply_markup(reply_markup=kb_submenu_position(conf))

@dp.callback_query(States.settings, F.data == "submenu_exit")
async def submenu_exit_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conf = await db.load_config()
    await callback.message.edit_reply_markup(reply_markup=kb_submenu_exit(conf))

@dp.callback_query(States.settings, F.data == "submenu_signal")
async def submenu_signal_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conf = await db.load_config()
    await callback.message.edit_reply_markup(reply_markup=kb_submenu_signal(conf))

@dp.callback_query(States.settings, F.data == "back_settings")
async def back_to_settings(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conf = await db.load_config()
    await callback.message.edit_reply_markup(reply_markup=kb_settings(conf))

@dp.callback_query(States.settings, F.data == "toggle_adaptive")
async def toggle_adaptive(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    conf = await db.load_config()
    await db.config_update(use_adaptive_sizing="0" if conf.use_adaptive_sizing else "1")
    conf = await db.load_config()
    await callback.message.edit_reply_markup(reply_markup=kb_submenu_exit(conf))

# ============================================================
# Position / trade limits
# ============================================================
@dp.callback_query(States.settings, F.data == "change_max_pos")
async def change_max_pos_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_max_pos)
    conf = await db.load_config()
    await callback.answer()
    await callback.message.answer(f"Max positions (1-20). Current: {conf.max_positions or 5}")

@dp.message(States.change_max_pos)
async def change_max_pos_value(message: Message, state: FSMContext):
    try:
        v = int(message.text.strip()); assert 1 <= v <= 20
    except Exception:
        await message.answer("Enter 1-20:"); return
    await db.config_update(max_positions=str(v))
    await message.answer(f"Max positions -> {v}")
    await state.set_state(States.settings); await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_entries_bar")
async def change_entries_bar_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_entries_bar)
    conf = await db.load_config()
    await callback.answer()
    await callback.message.answer(f"Max entries per bar (1-10). Current: {conf.max_entries_per_bar or 2}")

@dp.message(States.change_entries_bar)
async def change_entries_bar_value(message: Message, state: FSMContext):
    try:
        v = int(message.text.strip()); assert 1 <= v <= 10
    except Exception:
        await message.answer("Enter 1-10:"); return
    await db.config_update(max_entries_per_bar=str(v))
    await message.answer(f"Entries/bar -> {v}")
    await state.set_state(States.settings); await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_cooldown")
async def change_cooldown_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_cooldown)
    conf = await db.load_config()
    await callback.answer()
    await callback.message.answer(f"Cooldown bars (0-20). Current: {conf.symbol_cooldown or 3}")

@dp.message(States.change_cooldown)
async def change_cooldown_value(message: Message, state: FSMContext):
    try:
        v = int(message.text.strip()); assert 0 <= v <= 20
    except Exception:
        await message.answer("Enter 0-20:"); return
    await db.config_update(symbol_cooldown=str(v))
    await message.answer(f"Cooldown -> {v} bars")
    await state.set_state(States.settings); await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_interval")
async def change_interval_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_interval)
    conf = await db.load_config()
    await callback.answer()
    await callback.message.answer(
        f"⏱ <b>Выбери таймфрейм:</b>\n\n"
        f"<code>1m  5m  15m  30m</code> — для тестирования сделок\n"
        f"<code>1h  4h  8h  1d</code>  — для торговли\n\n"
        f"⚠️ <b>Внимание:</b> смена вступит в силу на следующей свече.\n"
        f"Текущий: <b>{conf.interval or '4h'}</b>",
        parse_mode="HTML"
    )

@dp.message(States.change_interval)
async def change_interval_value(message: Message, state: FSMContext):
    VALID = ("1m", "5m", "15m", "30m", "1h", "4h", "8h", "1d")
    v = message.text.strip().lower()
    if v not in VALID:
        await message.answer(f"❌ Допустимые значения: {' '.join(VALID)}")
        return
    await db.config_update(interval=v)
    note = " ⚠️ (тест-режим)" if v in ("1m", "5m", "15m", "30m") else ""
    await message.answer(f"✅ Interval → <b>{v}</b>{note}\n\nСмена вступит в силу на следующей свече.", parse_mode="HTML")
    await state.set_state(States.settings)
    await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_signal_mode")
async def change_signal_mode_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_signal_mode)
    conf = await db.load_config()
    await callback.answer()
    await callback.message.answer(f"Signal mode: structured or tv. Current: {conf.signal_mode or 'structured'}")

@dp.message(States.change_signal_mode)
async def change_signal_mode_value(message: Message, state: FSMContext):
    v = message.text.strip().lower()
    if v not in ("structured","tv"):
        await message.answer("Enter: structured or tv"); return
    await db.config_update(signal_mode=v)
    await message.answer(f"Signal mode -> {v}")
    await state.set_state(States.settings); await cmd_settings(message, state)

# ATR helpers
async def _ask_atr(callback, state, next_state, label, current):
    await state.set_state(next_state); await callback.answer()
    await callback.message.answer(f"Enter {label} (0.5-15.0). Current: {current}")

async def _save_atr(message, state, key, label):
    try:
        v = float(message.text.strip().replace(",",".")); assert 0.5 <= v <= 15.0
    except Exception:
        await message.answer("Enter 0.5-15.0:"); return
    await db.config_update(**{key: str(v)})
    await message.answer(f"{label} -> {v}")
    await state.set_state(States.settings); await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_stop_atr")
async def csa_h(c, s): conf = await db.load_config(); await _ask_atr(c, s, States.change_stop_atr, "Stop ATR", conf.stop_atr)
@dp.message(States.change_stop_atr)
async def csav_h(m, s): await _save_atr(m, s, "stop_atr", "Stop ATR")

@dp.callback_query(States.settings, F.data == "change_take1_atr")
async def ct1a_h(c, s): conf = await db.load_config(); await _ask_atr(c, s, States.change_take1_atr, "TP1 ATR", conf.take1_atr)
@dp.message(States.change_take1_atr)
async def ct1v_h(m, s): await _save_atr(m, s, "take1_atr", "TP1 ATR")

@dp.callback_query(States.settings, F.data == "change_take2_atr")
async def ct2a_h(c, s): conf = await db.load_config(); await _ask_atr(c, s, States.change_take2_atr, "TP2 ATR", conf.take2_atr)
@dp.message(States.change_take2_atr)
async def ct2v_h(m, s): await _save_atr(m, s, "take2_atr", "TP2 ATR")

@dp.callback_query(States.settings, F.data == "change_partial_pct")
async def cppa_h(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_partial_pct); conf = await db.load_config(); await callback.answer()
    await callback.message.answer(f"Partial exit % at TP1 (1-50). Current: {float(conf.partial_exit_pct or 0.1)*100:.0f}%")

@dp.message(States.change_partial_pct)
async def cppv_h(message: Message, state: FSMContext):
    try:
        v = float(message.text.strip().replace("%","")); assert 1 <= v <= 50
    except Exception:
        await message.answer("Enter 1-50:"); return
    await db.config_update(partial_exit_pct=str(v/100))
    await message.answer(f"Partial exit -> {v:.0f}%")
    await state.set_state(States.settings); await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_max_pos_usdt")
async def cmpu_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.change_max_pos_usdt)
    conf = await db.load_config()
    await callback.answer()
    cur = float(conf.manual_equity or 0)
    await callback.message.answer(
        f"💰 <b>Симуляция equity (USDT)</b>\n\n"
        f"Testnet Binance даёт $10,000+ автоматически, что делает позиции огромными.\n"
        f"Используй этот параметр чтобы симулировать работу с нужным депозитом.\n"
        f"Риск-менеджмент работает корректно: 1% от указанной суммы.\n\n"
        f"Текущее: <b>{'0 (берётся реальный баланс)' if cur == 0 else f'${cur:.0f}'}</b>\n"
        f"Введи сумму в USDT (напр. <code>200</code>) или <code>0</code> для реального баланса:",
        parse_mode="HTML"
    )

@dp.message(States.change_max_pos_usdt)
async def cmpu_value(message: Message, state: FSMContext):
    try:
        v = float(message.text.strip())
        assert v >= 0
    except Exception:
        await message.answer("❌ Введи число ≥ 0 (0 = реальный баланс):"); return
    await db.config_update(manual_equity=str(v))
    label = f"${v:.0f}" if v > 0 else "0 (реальный баланс)"
    await message.answer(f"✅ Equity (sim) → <b>{label}</b>", parse_mode="HTML")
    await state.set_state(States.settings); await cmd_settings(message, state)

# Threshold helpers
async def _ask_thresh(callback, state, next_state, label, current):
    await state.set_state(next_state); await callback.answer()
    await callback.message.answer(f"Enter {label} (0.0-1.0). Current: {current}")

async def _save_thresh(message, state, key, label):
    try:
        v = float(message.text.strip().replace(",",".")); assert 0.0 <= v <= 1.0
    except Exception:
        await message.answer("Enter 0.0-1.0:"); return
    await db.config_update(**{key: str(v)})
    await message.answer(f"{label} -> {v}")
    await state.set_state(States.settings); await cmd_settings(message, state)

@dp.callback_query(States.settings, F.data == "change_trend_min")
async def ctma_h(c, s): conf = await db.load_config(); await _ask_thresh(c, s, States.change_trend_min, "Trend min", conf.struct_trend_min)
@dp.message(States.change_trend_min)
async def ctmv_h(m, s): await _save_thresh(m, s, "struct_trend_min", "Trend min")

@dp.callback_query(States.settings, F.data == "change_mom_min")
async def cmma_h(c, s): conf = await db.load_config(); await _ask_thresh(c, s, States.change_mom_min, "Momentum min", conf.struct_momentum_min)
@dp.message(States.change_mom_min)
async def cmmv_h(m, s): await _save_thresh(m, s, "struct_momentum_min", "Momentum min")

@dp.callback_query(States.settings, F.data == "change_timing_min")
async def ctia_h(c, s): conf = await db.load_config(); await _ask_thresh(c, s, States.change_timing_min, "Timing min", conf.struct_timing_min)
@dp.message(States.change_timing_min)
async def ctiv_h(m, s): await _save_thresh(m, s, "struct_timing_min", "Timing min")

@dp.callback_query(States.settings, F.data == "change_comp_min")
async def ccma_h(c, s): conf = await db.load_config(); await _ask_thresh(c, s, States.change_comp_min, "Composite min", conf.struct_composite_min)
@dp.message(States.change_comp_min)
async def ccmv_h(m, s): await _save_thresh(m, s, "struct_composite_min", "Composite min")


# ============================================================
# Catch-all handlers — MUST be last
# ============================================================
@dp.callback_query()
async def catch_callback(callback: CallbackQuery):
    await callback.answer("Unknown action.")


@dp.edited_message()
async def catch_edited(message: Message):
    pass


@dp.message()
async def catch_all(message: Message):
    await message.answer(
        "🤔 Unknown command. Use /start to open the menu.",
        reply_markup=kb_main(),
    )


# ============================================================
# Runner — called from main.py
# ============================================================
async def run(ini, client_ref=None):
    """Start the Telegram bot. Blocks until bot stops."""
    global bot, ADMINS, _client

    token      = os.getenv("TG_TOKEN", ini["TG"]["token"])
    admins_str = os.getenv("TG_ADMINS", ini["TG"]["admins"])
    ADMINS[:] = [int(x.strip()) for x in admins_str.split(",") if x.strip()]
    _client    = client_ref

    # Register middlewares: Admin check first, then Nav state reset
    dp.message.middleware(AdminMiddleware())
    dp.callback_query.middleware(AdminMiddleware())
    dp.message.middleware(NavMiddleware())

    bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info(f"✅ Telegram bot started (admins: {ADMINS})")

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
