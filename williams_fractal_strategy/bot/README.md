# Live bot — Williams Fractal breakout on Binance USDT-M Futures

A live/testnet trading bot for the fractal breakout strategy in
`../src` — same signal logic, same stop/take-profit parameters as
`../run_backtest.py`, wired to a real (or simulated) exchange via
WebSocket market data and a REST order-placement gateway.

> **Disclaimer:** Educational project. Not financial advice. Futures
> trading with leverage carries a high risk of loss, including total
> and rapid loss of capital. Verify everything on testnet — repeatedly,
> under different conditions — before ever pointing this at a funded
> live account. Telegram/notifications are deliberately not included
> here; add your own if you want them.

## Module layout

The module names below deliberately follow the same top-level naming
you'd recognize from a larger production bot (`main` / `db` /
`config` / `risk` / `gateway` / ...) — but this project only includes
what a single-strategy bot actually needs to trade for real. No
multi-strategy registry, no PostgreSQL server to run, no notification
system, no health-check subsystem — clone it and it runs.

| File | Purpose |
|---|---|
| `main.py` | Entry point + orchestration: signal → size → entry → protection → fill handling → reconciliation |
| `config.py` | Settings from environment / `.env` — one `EXECUTION_MODE` flag (`dry_run` / `testnet` / `live`) fully determines which endpoints are used |
| `db.py` | **SQLite**, not PostgreSQL — zero setup, single file, still gives you real persistence and a trade history table |
| `models.py` | Shared dataclasses (`PositionState`, `SizingResult`, `TradeRecord`) |
| `strategy.py` | Turns a bar window into a signal — imports `../src/fractals.py` unchanged |
| `risk.py` | Stop/take-profit price + risk-based position sizing (mirrors `../src/backtest.py`) |
| `gateway.py` | Real Binance USDT-M Futures REST client (signed requests) |
| `dry_run_gateway.py` | In-memory simulated exchange — same method surface as `gateway.py`, no network |
| `websocket.py` | Market-data WebSocket (combined kline stream), staleness detection, reconnect/backoff |
| `user_stream.py` | User-data WebSocket (order/position fills), listenKey lifecycle |
| `bars.py` | Rolling window of **closed** candles per symbol |
| `filters.py` | Exchange symbol filters (stepSize/minQty/minNotional) + rounding |
| `utils.py` | Logging setup, `.env` loader |
| `scripts/verify_live_pipeline.py` | No-network end-to-end verification (see below) |

## How a trade happens

1. `websocket.py` receives a closed candle (`"x": true` in Binance's
   kline event — a still-forming candle is never acted on).
2. `main.py` calls `strategy.check_signal()`, which runs
   `../src/fractals.generate_signals()` over the rolling window and
   reads the signal on the newest bar — the exact same function the
   backtest uses.
3. If there's a signal and no open position for that symbol (checked
   in `db.py`), `risk.compute_sizing()` resolves the stop price
   (structure/ATR/percent, same as the backtest), the take-profit
   price (`reward_risk_ratio` × stop distance), and a risk-based
   quantity — rounded to the exchange's stepSize and bumped up to
   min_qty/min_notional if needed, capped by `max_leverage`.
4. `main.py` sends a MARKET entry, then a `STOP_MARKET` and a
   `TAKE_PROFIT_MARKET`, both `closePosition=true` (the
   Binance-idiomatic way to say "close the whole position on
   trigger" — deliberately not `reduceOnly`+quantity, which Binance
   rejects when combined with `closePosition`).
5. `user_stream.py` receives the fill event when the stop or take
   triggers; `main.py` records the closed trade in `db.py`'s history
   table, clears the open position, and cancels the sibling order.
6. Every `POLL_RECONCILE_SECONDS`, `main.py` re-checks the exchange's
   actual position against `db.py` and fixes drift — this is what
   saves you if the bot was offline when a stop filled.

## Quick start

```bash
cd bot
pip install -r ../requirements.txt -r requirements.txt
cp .env.example .env
```

### Step 1 — no network at all

```bash
EXECUTION_MODE=dry_run python3 scripts/verify_live_pipeline.py
```

Builds a hand-crafted breakout pattern and runs it through the real
signal → sizing → order-placement code: checks that a stop-loss lands
below entry and a take-profit lands above it (for a LONG), that sizing
respects the exchange's min_qty/min_notional, that a simulated fill
correctly closes the position, and that the closed trade lands in the
SQLite trade history. No API keys, no internet — if this fails,
nothing downstream will work either.

### Step 2 — real testnet smoke test

Edit `.env`:
```
EXECUTION_MODE=testnet
BINANCE_API_KEY=<your own fresh testnet key>
BINANCE_API_SECRET=<your own fresh testnet secret>
DEBUG_MODE=true
```
Get testnet keys at https://testnet.binancefuture.com — a separate
account from your real Binance login, funded with fake balance.
**Never reuse real keys here, and never paste real keys into a chat
or document to have someone else look at them.**

```bash
python3 main.py --once
```

With `DEBUG_MODE=true`, this forces one signal on the first symbol if
no real breakout is found, so you see a real entry + stop + take-profit
land on testnet in one pass instead of waiting for a genuine breakout.
Check the output and your testnet account's Futures order history —
confirm the stop and take orders are actually sitting there. Turn
`DEBUG_MODE` off once you've verified this.

### Step 3 — continuous run

```bash
python3 main.py
```

Runs until `Ctrl-C` / `SIGTERM`, reconnecting both WebSocket feeds
automatically on disconnect.

## Configuration

All settings are environment variables — see `.env.example` for the
full list with defaults. The risk/stop/take-profit parameters
(`RISK_PER_TRADE`, `STOP_MODE`, `REWARD_RISK_RATIO`, `MAX_LEVERAGE`,
...) share names and meaning with `../run_backtest.py`'s CLI flags —
backtest a configuration with `run_backtest.py` before running it here.

## Checking your trade history

```bash
python3 -c "from db import Database; d = Database('bot.db'); [print(r) for r in d.get_trade_history()]"
```

## What could not be verified from this environment

This was built and tested in a sandbox with no network access to
Binance at all (confirmed directly — Binance's API domains return
connection failures through the egress proxy here). Everything that
can be verified without a live exchange connection has been: signal
generation, sizing math (min_qty/min_notional bump, stepSize
rounding), order-placement call shape (stop below/take above entry
for LONG, and the mirror for SHORT), fill-event handling, and SQLite
trade-history recording — all exercised end to end through
`scripts/verify_live_pipeline.py` against the in-memory simulator.

What that script *cannot* verify: actual Binance testnet connectivity,
real fill behavior, and testnet's occasional quirks around stop-order
rejection. That last step has to happen on your machine, with your own
testnet keys — Step 2 above is exactly that.
