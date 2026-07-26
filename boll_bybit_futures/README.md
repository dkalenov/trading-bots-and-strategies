# Bollinger Bands Strategy — Bybit Futures

Backtesting infrastructure + live trading bot for Bollinger Bands strategy on Bybit USDT perpetual futures.

**Disclaimer:** Educational purposes only. Crypto trading carries high risk of total loss.

## How It Works

Bollinger Bands uses SMA ± standard deviation to define dynamic support/resistance:
- Upper band = SMA + 2×σ, Lower band = SMA − 2×σ
- BUY signal: price crosses above upper band (breakout)
- SELL signal: price crosses below lower band (breakdown)
- 4 strategy variants: basic, RSI filter, squeeze breakout, mean reversion

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | 4 BB variants + RSI, ATR, BB indicators |
| `backtester.py` | Realistic backtester (fees, slippage, funding) |
| `live_testnet.py` | WebSocket bot for Bybit Futures Testnet |
| `main.py` | CLI: backtest, optimize, monthly analysis |
| `batch_fast.py` | Batch backtest ALL Bybit perpetuals (parallel) |
| `walk_forward.py` | Walk-forward optimization |
| `monte_carlo.py` | Monte Carlo validation |
| `utils.py` | Bybit V5 data download + cache |
| `config.py` | Config loader |
| `verify_backtest.py` | No look-ahead bias, correct PnL |
| `test_edge_cases.py` | Edge-case tests |

## Quick Start

```bash
pip install -r requirements.txt

# Backtest
python main.py --symbol BTCUSDT --interval 1h --variant basic

# All 4 variants
python main.py --symbol BTCUSDT --interval 1h --all-variants

# Verification
python verify_backtest.py
python test_edge_cases.py

# Live testnet
set BYBIT_TESTNET_API_KEY=your_key
set BYBIT_TESTNET_API_SECRET=your_secret
python live_testnet.py --symbol COREUSDT --interval 1h --variant basic --bb-timeperiod 30 --tp-multiplier 4.0 --sl-multiplier 1.5 --mainnet-data --live --debug
```

## Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `bb_timeperiod` | 20 | BB SMA lookback period |
| `bb_nbdevup` | 2.0 | Upper band std dev multiplier |
| `bb_nbdevdn` | 2.0 | Lower band std dev multiplier |
| `tp_multiplier` | 3.0 | Take profit = entry ± (BB width × 3.0 / 2) |
| `sl_multiplier` | 1.5 | Stop loss = entry ± (BB width × 1.5 / 2) |
| `leverage` | 20x | Futures leverage |
| `risk_pct` | 1% | Risk per trade |
| `commission` | 0.04% | Taker fee |
| `slippage` | 0.02% | Estimated slippage |
| `funding_rate` | 0.01% | Every 8h |

## Architecture

```
REST API (klines)  ──→  BB calculation (SMA ± σ)
                            ↓
WebSocket (kline)  ──→  Real-time price ──→  Signal check
                            ↓
WebSocket (kline)  ──→  Candle close ──→  BB recalc
                            ↓
REST API  ──→  Market order + SL (algo) + TP (algo)
```

- **kline WS**: real-time price ticks + candle close events
- **SL**: Conditional order via `/v5/order/create` (triggerPrice)
- **TP**: Conditional order via `/v5/order/create` (triggerPrice)
- **`--mainnet-data`**: Use mainnet for price data (testnet has fake data for some symbols)

## Strategy Variants

| Variant | Description |
|---------|-------------|
| `basic` | BB crossover: BUY above upper, SELL below lower |
| `rsi_filter` | BB + RSI momentum confirmation (>55 BUY, <45 SELL) |
| `squeeze` | BB squeeze breakout (narrow BB → expansion) |
| `mean_reversion` | Enter at band touch, exit at middle band |

## Backtest Results

### BTCUSDT 1h (2024-01 to 2025-06, 13000 candles)

| Variant | Return | Sharpe | WR | Trades | MaxDD | PF |
|---------|--------|--------|-----|--------|-------|-----|
| basic | -11.98% | -0.257 | 36.1% | 460 | -27.46% | 0.95 |
| rsi_filter | -7.52% | -0.122 | 36.1% | 441 | -24.74% | 0.97 |
| squeeze | -18.33% | -0.983 | 32.3% | 127 | -19.93% | 0.79 |
| mean_reversion | -99.92% | -20.584 | 22.9% | 1657 | -99.92% | 0.18 |

### Batch: 243 Bybit USDT perpetuals

- **92 profitable (38%)** with min 10 trades
- Top by Sharpe: HUMAUSDT (7.66), BABYUSDT (5.47), BANKUSDT (4.48)
- Top by Return: COREUSDT (+136%), JASMYUSDT (+66%), AEROUSDT (+61%)

### Walk-Forward + Monte Carlo (COREUSDT)

| Metric | Value |
|--------|-------|
| WF Avg Test Return | +18.05% |
| WF Efficiency | 0.808 |
| MC Permutation Profit% | 100% |
| MC Bootstrap 50% Prob | 98.3% |
| Verdict | **ROBUST** |

## Verified

- No look-ahead bias
- Correct PnL math (gross − commission − slippage − funding)
- SL/TP execution respected
- Deterministic results (same output on re-run)
- All 4 variants pass edge case tests

## Contacts

Telegram: @KDR_98
