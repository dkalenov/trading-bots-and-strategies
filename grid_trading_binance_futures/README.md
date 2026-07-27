# Grid Trading Bot — Binance USD-M Futures

A grid-trading bot and backtester for Binance USD-M Futures. This version is a
full rewrite of the original project after a code audit found that the
backtester and the live bot implemented two **different, disconnected
strategies** (see "What changed" below) — the numbers in the old README did
not describe what the live bot would actually have done.

## Strategy

1. While **flat**, the bot places `n_levels` buy limit orders below the
   current price and `n_levels` sell limit orders above it, evenly spaced
   `proportion`% apart.
2. The first side to fill sets the **direction** (a buy fill → LONG, a sell
   fill → SHORT). The untouched orders on the *opposite* side are cancelled
   immediately — you don't want a stray grid order flipping your position.
3. Same-direction grid orders are left resting, so the position can
   **average in** further (up to `n_levels` fills total) if price keeps
   moving against it.
4. A single **take-profit** order for the whole averaged position is kept in
   sync every poll. `tp_pct` is a **% ROI on the margin used**, not a raw
   price-move % — it already accounts for leverage, the same way "ROI%"
   works in the Binance app. With `leverage=1` the two are close in
   practice.
5. An optional **stop-loss** (`stop_loss_pct`, same ROI-based units) can
   force-close the position early. It's `None` (disabled) by default,
   matching the classic "grid bot has no stop-loss" behaviour — but see
   **Risk** below for why you probably want to set one.
6. Once the position is flat again (TP, stop-loss, or liquidation), a fresh
   grid is drawn around the current price.

`grid_strategy.py` contains this logic as pure functions with no exchange
client and no backtest-only bits — both `backtest.py` and `binance_bot.py`
import from it, so they cannot silently diverge again.

## Project structure

| File | Purpose |
|---|---|
| `grid_strategy.py` | Shared strategy math (grid levels, TP/SL price, liquidation estimate) — single source of truth |
| `backtest.py` | Historical simulation against Binance Vision kline data |
| `binance_bot.py` | Live execution on Binance USD-M Futures |
| `main.py` | Entry point — configure symbols/parameters and start the bot(s) |
| `config.py` | Reads `BINANCE_API_KEY` / `BINANCE_API_SECRET` from the environment |
| `klines/` | Cached monthly kline data (auto-downloaded on first backtest run) |

## Setup

```bash
pip install -r requirements.txt

export BINANCE_API_KEY="your-testnet-key"
export BINANCE_API_SECRET="your-testnet-secret"
```

Get testnet keys at https://testnet.binancefuture.com. **Do not use mainnet
keys until you've watched the bot run correctly on testnet for a while and
understand the Risk section below.**

## Running the backtest

```bash
python backtest.py --symbol BTCUSDT --interval 1h --start 2024-01 --end 2025-06 \
    --n-levels 10 --proportion 1.5 --tp 3 --stop-loss 10 --leverage 1 --capital 10000
```

| Flag | Default | Meaning |
|---|---|---|
| `--symbol` | `BTCUSDT` | Futures symbol |
| `--interval` | `1h` | Kline interval |
| `--start` / `--end` | `2024-01` / `2025-06` | `YYYY-MM` inclusive range |
| `--n-levels` | `10` | Buy levels **and** sell levels |
| `--proportion` | `1.5` | Grid spacing, % between adjacent levels |
| `--volume` | `0.05` | Order size per level, in base asset |
| `--tp` | `3.0` | Take-profit, % ROI on margin used |
| `--stop-loss` | disabled | Stop-loss, % ROI loss on margin used |
| `--leverage` | `1` | Account leverage (affects margin & TP/SL price distance) |
| `--capital` | `10000` | Starting balance, USDT |
| `--commission` | `0.04` | Taker commission, % of notional per fill |
| `--slippage` | `0.05` | Slippage, % applied against you on every fill |

Kline data is cached under `klines/` (already includes BTCUSDT 1h,
Jan 2022 – Jun 2025 — validated gap-free, 30,648 candles, no duplicates).
Other symbols/ranges are downloaded automatically from
`data.binance.vision` on first use.

## Running the live bot

Edit the `SYMBOLS` list and `TESTNET` flag in `main.py`, then:

```bash
python main.py
```

Each symbol runs in its own thread, polling every `POLL_INTERVAL` seconds
(default 5s) — the loop sleeps between iterations and backs off
exponentially on errors, instead of hammering the REST API.

## Honest backtest results (BTCUSDT, 1h, after the fix)

Same grid (`n_levels=10`, `proportion=1.5%`, `tp=3%` ROI, `leverage=1`),
$10,000 starting capital, run separately per calendar period:

| Period | No stop-loss | With 10% stop-loss |
|---|---|---|
| 2022 (bear market) | **-47.90%**, max DD 62.1%, 94.4% win rate | **+1.65%**, max DD 27.5%, 88.5% win rate |
| 2023 | **-122.23%**, max DD 133.4% (1 stuck cycle) | **-22.90%**, max DD 34.2%, 84.3% win rate |
| 2024 | **-112.60%**, max DD 143.8% (1 stuck cycle) | **-1.71%**, max DD 42.0%, 85.1% win rate |
| 2025 H1 (calmer market) | **+30.53%**, max DD 19.1%, 94.4% win rate | **+12.89%**, max DD 30.6%, 84.1% win rate |
| Full 2022-01 → 2025-06, one continuous run | **-126.99%**, max DD 126.3%, 97.1% win rate (34/35 cycles won) | **-26.37%**, max DD 36.9%, 85.1% win rate |

These are real, reproducible numbers from the data in `klines/` — run
`backtest.py` yourself with the commands above to verify them.

**Why the win rate and the total return disagree so sharply:** in the
"no stop-loss" full-period run, 34 of 35 cycles closed at a small profit —
and then a single SHORT position, opened in February 2024 at an average
entry around $46,400, never found a take-profit because BTC rallied
straight through to over $107,000 by June 2025. With no stop-loss and no
liquidation at 1x leverage, that one position stayed open for the rest of
the backtest and lost about $18,200 — more than the account's starting
capital. A 97% win rate strategy with unbounded position duration and no
exit plan for a sustained one-directional trend can still blow up the
account. **This is exactly the risk the old, buggy backtest hid** — its
take-profit logic wasn't actually tied to real entries, so it could close
out "trades" at arbitrary grid levels regardless of real exposure, and
never surfaced this failure mode at all.

## Risk (read this before using real funds)

- **No stop-loss by default.** A static grid with no stop-loss has
  theoretically unbounded loss on the side that gets "stuck" during a
  sustained trend, as shown above. Consider setting `stop_loss_pct`.
- **Liquidation is only roughly estimated** in the backtest
  (`entry * (1 ± 1/leverage)`, ignoring Binance's maintenance-margin
  tiers and fees) and is not actively managed in the live bot — the
  optional stop-loss is your own protection layer; Binance's actual
  liquidation engine is a separate, harsher backstop you do not want to
  rely on.
- **No funding-rate simulation.** Perpetual futures pay/receive funding
  every 8 hours; over a multi-month held position this can be a
  meaningful additional cost or benefit, and it's not modeled here.
  With leverage=1, this is proportionally very small.
- **Intra-candle fill order is a heuristic**, not certain knowledge: the
  backtest infers whether price moved low-then-high or high-then-low
  within a candle from whether the candle closed up or down. This
  reduces, but does not eliminate, optimistic look-ahead bias from OHLC
  data.
- **No partial fills / order-book depth.** Every fill assumes the full
  order size executes at the level price plus slippage.
- **One-way position mode assumed.** The live bot reads
  `futures_position_information` and sums `positionAmt` across whatever
  rows come back; it hasn't been tested against a hedge-mode account
  running separate LONG and SHORT positions simultaneously.
- Nothing here is financial advice. Test on testnet, understand the
  mechanics, and only risk money you can afford to lose.

## What changed from the original version (audit summary)

The earlier version of this project had `backtest.py` and `binance_bot.py`
implementing two unrelated strategies, plus several live-trading bugs:

- **`--tp` did nothing in the old backtester** — take-profit price came
  from a static grid level, never from the actual entry price, so the
  backtest wasn't testing the strategy the bot actually runs.
- **The old live bot's grid formula and the old backtest's grid formula
  were different functions** (a compounding per-level multiplier vs. a
  flat `%`), and the two files used the `proportion` parameter in
  incompatible units — the backtested grid was roughly 100x wider than
  the grid the default live config would have drawn.
- **`get_mark_price()` called the *spot* ticker endpoint** to price a
  *futures* grid. On testnet these are two entirely separate exchanges
  (`testnet.binance.vision` vs `testnet.binancefuture.com`).
- Position size was compared with a hardcoded string (`!= "0.000"`),
  which silently breaks for symbols whose API response uses a different
  decimal precision.
- `cal_tp_level` indexed a filtered DataFrame with `.iloc[index]` using
  the *original* (non-reset) index labels — works by luck in one-way
  mode with a single position row, raises `IndexError` in hedge mode.
- The live polling loop had no rate limiting (`while True` with no
  `sleep`), risking an IP ban from Binance.
- `requirements.txt` was missing `numpy`, `python-dateutil`, and `pytz`,
  which `backtest.py` actually imports.

This rewrite fixes all of the above, adds an optional stop-loss and a
simplified liquidation check to the backtester, gives orders proper
`clientOrderId` tags so the live bot always knows which resting order is
which, and pulls real price/quantity precision from
`futures_exchange_info` instead of a hardcoded decimal count.
