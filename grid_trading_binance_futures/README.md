# Grid Trading Bot — Binance Futures

Automated grid trading bot that places limit orders at fixed price intervals for Binance Futures.

**DISCLAIMER:** This is for educational and research purposes only. Trading cryptocurrencies involves high risk of total loss. The author is not responsible for any financial losses. Do not use with real money without fully understanding the risks.

## How It Works

Grid trading places a "grid" of limit orders around the current price:
- **Buy orders** placed at fixed intervals BELOW current price
- **Sell orders** placed at fixed intervals ABOVE current price
- When a buy fills → a sell is placed at the take-profit level
- When a sell fills → a buy is placed at the lower grid level
- The bot profits from price oscillation within the grid range

### Grid Trading vs Trend Following

| Feature | Grid Trading | Trend Following |
|---------|-------------|-----------------|
| Best market | Sideways / ranging | Trending |
| Entry trigger | Pre-set price levels | Indicator signal |
| Profit source | Price oscillation | Directional moves |
| Risk | Large drawdown in strong trend | Whipsaws in sideways |
| Win rate | High (~60-80%) | Low (~25-40%) |
| Avg win | Small (grid spacing) | Large (trend extension) |

### When Grid Trading Works

- **Ranging markets** — price oscillates between support and resistance
- **High volatility** — more grid fills = more profit
- **Mean-reverting assets** — price tends to return to mean

### When Grid Trading Fails

- **Strong trends** — all grid levels on one side fill, large drawdown
- **Low volatility** — few fills, capital sits idle
- **Breakouts** — grid gets trapped on wrong side

## Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point — creates bot instances for BTCUSDT and ETHUSDT |
| `binance_bot.py` | Core logic: grid drawing, order management, TP calculation |
| `backtest.py` | Grid backtester with historical Binance data |
| `config.py` | API key/secret configuration (env vars) |
| `requirements.txt` | Python dependencies |

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Backtest
python backtest.py --symbol BTCUSDT --interval 1h --n-levels 10 --tp 5

# Live bot (testnet)
export BINANCE_API_KEY=your_testnet_key
export BINANCE_API_SECRET=your_testnet_secret
python main.py
```

## Parameters

### Grid Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `n` | 15 | Number of grid levels per side |
| `proportion` | 1.5% | Grid spacing — distance between levels |
| `volume` | 0.05 | Trade volume per grid level (in base asset) |
| `tp` | 3% | Take profit percentage per grid cycle |
| `no_of_decimals` | 1 | Price rounding precision |

### Backtest Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `--symbol` | BTCUSDT | Trading pair |
| `--interval` | 1h | Candle interval |
| `--start` | 2024-01 | Backtest start date |
| `--end` | 2025-06 | Backtest end date |
| `--n-levels` | 15 | Grid levels per side |
| `--proportion` | 1.5 | Grid spacing % |
| `--tp` | 3.0 | Take profit % |
| `--capital` | $10,000 | Initial capital |
| `--slippage` | 0.05% | Slippage per trade |

## Architecture

```
REST API (price)  ──→  Grid Calculation
                         ↓
Limit Orders  ──→  Binance Futures API
                         ↓
Order Fill Detection  ──→  Opposite Side Cancel + TP Placement
                         ↓
Grid Redraw  ──→  New cycle
```

- **Grid drawing**: N sell orders above, N buy orders below current price
- **Order management**: When buy fills, cancel opposite sells and place TP
- **Position tracking**: Uses Binance position information API
- **Multi-symbol**: Runs BTCUSDT and ETHUSDT in parallel threads

## Backtest Results

BTCUSDT 1h, 15 levels, 1.5% spacing, 0.05 BTC/level, 3% TP, $10,000 capital:

| Period | Return | Max DD | Win Rate | Trades | Profit Factor |
|--------|--------|--------|----------|--------|---------------|
| 2022 (Bear) | -4.32% | 41.49% | 48.5% | 206 | 0.95 |
| 2023 (Sideways) | +38.39% | 8.89% | 81.6% | 98 | 10.00 |
| 2024-2025 (Bull) | +89.29% | 29.76% | 70.2% | 168 | 2.51 |

### Key Observations

- **Sideways markets are ideal** — 81.6% win rate, 10x profit factor, minimal drawdown
- **Bull markets work well** — 70% win rate, strong returns, but higher drawdown (29%)
- **Bear markets are dangerous** — grid buys accumulate, forced closes at losses, 41% drawdown
- **Forced closes** happen when price drops through all grid levels without recovering

> Note: Grid trading backtests simulate fill events but cannot perfectly replicate
> order book dynamics, partial fills, or real-time slippage. Results are directional estimates.

## What is Implemented

### binance_bot.py — Core Bot:

- Grid drawing with configurable levels and spacing
- Limit order placement (buy + sell)
- Order cancellation (all, buy-only, sell-only)
- Position direction detection (LONG/SHORT/FLAT)
- Take-profit calculation and placement
- Dynamic TP adjustment as position changes
- Multi-symbol support via threading

### backtest.py — Backtester:

- Historical data download from Binance Vision (futures)
- Grid simulation with fill detection
- PnL tracking per grid cycle
- Win rate and average PnL statistics

## What is NOT Implemented

- No stop-loss (relies on grid range)
- No trailing stop
- No dynamic grid adjustment based on volatility
- No position size optimization
- No Telegram notifications
- No order book simulation (limit order fills only)

## Contacts

Telegram: @KDR_98
LinkedIn: dmitrii-kalenov
Email: drkalenov@gmail.com
