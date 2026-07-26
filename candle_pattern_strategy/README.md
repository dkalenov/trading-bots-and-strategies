# Candle Pattern Strategy — Bybit Futures

Candlestick pattern recognition bot with ATR-based risk management for Bybit Futures.

**Disclaimer:** Educational purposes only. Crypto trading carries high risk of total loss.

## How It Works

- Detects 11 candlestick patterns: Engulfing, Hammer, Morning/Evening Star, Three White Soldiers/Black Crows, Piercing Line, Dark Cloud Cover, Harami Cross
- Patterns are scored by weighted strength (0.9–1.4) with optional volume boost
- **BUY signal:** Bullish pattern detected (engulfing, hammer, morning star, etc.)
- **SELL signal:** Bearish pattern detected (bearish engulfing, evening star, etc.)
- ATR-based stop-loss and take-profit for risk management
- **Trend filter:** EMA-50/EMA-200 — only trade with the trend (significantly improves results)

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | Pattern detection + signal generation (11 patterns) |
| `backtest.py` | Backtester with Bybit data download + caching |
| `main.py` | Live trading bot (WebSocket + REST, Bybit Futures) |
| `Pattern_strategy.ipynb` | Research notebook (Colab) |
| `ML_Trading_Bot.ipynb` | ML-based pattern recognition |
| `requirements.txt` | Python dependencies |

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Backtest (optimized config with trend filter)
python backtest.py --symbol BTCUSDT --interval 1h --start 2024-01 --end 2025-06

# Backtest baseline (no filters)
python backtest.py --symbol BTCUSDT --interval 1h --start 2024-01 --end 2025-06 --baseline

# Live bot (dry run)
python main.py --symbol BTCUSDT --interval 1h

# Live bot (testnet)
export BYBIT_TESTNET_API_KEY=your_key
export BYBIT_TESTNET_API_SECRET=your_secret
python main.py --symbol BTCUSDT --interval 1h --testnet --live --debug
```

## Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `sl_atr` | 2.0 | Stop loss = N x ATR |
| `tp_atr` | 4.0 | Take profit = N x ATR |
| `atr_period` | 14 | ATR calculation period |
| `min_body_atr` | 0.15 | Min candle body size (ATR fraction) |
| `risk_pct` | 1% | Risk per trade |
| `leverage` | 10x | Futures leverage |
| `min_strength` | 1.3 | Min pattern strength to trade |
| `ema_fast` | 50 | Fast EMA for trend filter |
| `ema_slow` | 200 | Slow EMA for trend filter |
| `min_atr_pct` | 0.3% | Min ATR % of price (volatility filter) |

## Architecture

```
WebSocket (kline)  ──→  Pattern Detection (11 patterns)
                              ↓
                     Signal + Strength scoring
                              ↓
                     Market Order  ──→  SL (ATR-based)
                                       TP (ATR-based)
```

## Pattern Weights

| Pattern | Weight | Direction |
|---------|--------|-----------|
| Three White Soldiers | 1.4 | Bull |
| Three Black Crows | 1.4 | Bear |
| Morning Star | 1.3 | Bull |
| Evening Star | 1.3 | Bear |
| Bullish Engulfing | 1.2 | Bull |
| Bearish Engulfing | 1.2 | Bear |
| Piercing Line | 1.0 | Bull |
| Dark Cloud Cover | 1.0 | Bear |
| Hammer | 0.9 | Bull |
| Inverted Hammer | 0.9 | Bear |
| Harami Cross | 0.8 | Reversal |

## Backtest Results

BTCUSDT 1h (2024-01 to 2025-06, 13,000 candles):

### Without filters (baseline)

| Metric | Value |
|--------|-------|
| Return | -99.0% |
| Trades | 1,523 |
| Win Rate | 40.5% |
| Max Drawdown | -99.0% |
| Profit Factor | 0.54 |

**Conclusion:** Pure candlestick patterns without filters are not profitable. Engulfing patterns dominate (63% of signals) but generate noise.

### With EMA Trend Filter + Optimized Parameters

| Metric | Value |
|--------|-------|
| Return | **+34.2%** |
| Trades | 329 |
| Win Rate | 39.2% |
| Max Drawdown | -16.9% |
| Sharpe Ratio | 1.024 |
| Profit Factor | 1.13 |

**Config:** SL=2.0x ATR, TP=4.0x ATR, EMA-50/200 trend filter, min_strength=1.3, min ATR 0.3%

### Key Findings

1. **Trend filter is critical** — reduces losses from -99% to profitable territory
2. **Wide TP (2:1 R:R)** — allows winners to run, compensates for lower win rate
3. **Pattern selection matters** — Three White Soldiers/Black Crows + Stars are most reliable
4. **Engulfing patterns are noise** — 63% of baseline signals but lowest edge
5. **329 trades over 18 months** — ~18 trades/month, reasonable frequency

## Contacts

Telegram: @KDR_98
