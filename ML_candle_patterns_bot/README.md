# ML Candle Patterns Bot

> **DISCLAIMER:** This is an educational project. Trading cryptocurrencies involves
> high risk of total loss. The author is not responsible for any financial losses.
> Do not use with real money without fully understanding the risks.

## What Is This

ML strategy that predicts the next candle direction using candlestick patterns.
GradientBoosting classifier trained on 13 technical patterns + indicators.

**Current result:** models perform better than random but remain unprofitable for live trading.

## How It Works

1. Load historical klines from Binance
2. Detect 13 candlestick patterns (doji, hammer, engulfing, etc.)
3. Train GradientBoosting on pattern features
4. Generate LONG/SHORT signals based on model predictions

## Candlestick Patterns

| # | Pattern | Type | Description |
|---|---------|------|-------------|
| 1 | Doji | Neutral | Body ≈ 0 (low volatility) |
| 2 | Hammer | Bullish | Long lower shadow, small body |
| 3 | Inverted Hammer | Bullish | Long upper shadow, small body |
| 4 | Bullish Engulfing | Bullish | Current candle engulfs previous (up) |
| 5 | Bearish Engulfing | Bearish | Current candle engulfs previous (down) |
| 6 | Piercing Line | Bullish | Opens below prev low, closes above midpoint |
| 7 | Dark Cloud Cover | Bearish | Opens above prev high, closes below midpoint |
| 8 | Morning Star | Bullish | 3 candles: drop → small body → rise |
| 9 | Evening Star | Bearish | 3 candles: rise → small body → drop |
| 10 | Three White Soldiers | Bullish | 3 consecutive bullish candles |
| 11 | Three Black Crows | Bearish | 3 consecutive bearish candles |
| 12 | Harami Cross | Neutral | Doji inside previous body |
| 13 | Bearish Harami | Bearish | Small body inside previous (down) |

## ML Model

- **Algorithm:** GradientBoosting (200 trees, depth=5, lr=0.1)
- **Features:** 13 patterns + returns + volatility + volume_ratio
- **Target:** next candle > current close (1=LONG, 0=SHORT)
- **Metrics:** Accuracy, Precision, Recall, F1, ROC-AUC

## Project Structure

| File | Description |
|------|-------------|
| `main.py` | Bot entry point — loads data, runs strategy, generates signals |
| `strategy.py` | Strategy logic — patterns + ML model combined |
| `data_loader.py` | Binance klines data loader |
| `backtest.py` | Backtester with SL/TP and metrics |
| `ML_Trading_Bot.ipynb` | Colab notebook (source) |
| `requirements.txt` | Python dependencies |

## Backtest

```bash
pip install -r requirements.txt
python backtest.py
```

Default parameters:
- Symbol: ADAUSDT
- Interval: 15m
- Period: 2022-09 — 2025-09
- SL: 2% | TP: 4%
- Commission: 0.1%

## Running

```bash
pip install -r requirements.txt

# Backtest
python backtest.py

# Demo
python main.py

# Or open notebook in Colab
```

## Framework Integration

For live trading, use the `algofactory_bot/` framework.

**IMPORTANT:** `algofactory_bot/` is a black box. DO NOT modify or change it.

Strategy connects as an adapter:
- `strategy.py` — pattern detection + ML prediction
- Signals (LONG/SHORT) are passed to the framework

## Contacts

- Telegram: [@KDR_98](https://t.me/KDR_98)
- LinkedIn: [dmitrii-kalenov](https://www.linkedin.com/in/dmitrii-kalenov)
- Email: drkalenov@gmail.com
