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

---

## Theory: Candlestick Patterns

### What Are Candlestick Patterns

Candlestick patterns are visual formations on price charts that indicate potential future price movements. Each candle represents four price points: **Open, High, Low, Close (OHLC)** within a time period.

A candle has:
- **Body** — the area between Open and Close
- **Upper shadow** — the line from the top of the body to the High
- **Lower shadow** — the line from the bottom of the body to the Low

### Bullish vs Bearish Patterns

| Type | Signal | Meaning |
|------|--------|---------|
| **Bullish** | LONG | Price may go up — buy signal |
| **Bearish** | SHORT | Price may go down — sell signal |
| **Neutral** | Wait | No clear direction — indecision |

### All 13 Detected Patterns

#### 1. Doji (Neutral)

```
  |
 ─┼─    Body ≈ 0 (Open ≈ Close)
  |
```

A Doji forms when Open and Close are nearly equal. It signals **market indecision** — neither buyers nor sellers are in control. Often appears before a trend reversal.

**Detection:** `body_abs <= 10% * range` (where range = High - Low)

---

#### 2. Hammer (Bullish)

```
  |
 ─┤      Small upper shadow
  │
  │      Long lower shadow (≥ 2x body)
  │
  └
```

Hammer appears after a downtrend. The long lower shadow shows that sellers pushed the price down, but buyers pushed it back up. Indicates potential **trend reversal upward**.

**Detection:** `lower_shadow >= 2 * body AND upper_shadow <= 0.3 * body`

---

#### 3. Inverted Hammer (Bullish)

```
  ┌
  │      Long upper shadow (≥ 2x body)
  │
  │
 ─┤      Small lower shadow
  │
```

Same as Hammer but inverted. Appears after a downtrend. The long upper shadow shows buying pressure, but sellers couldn't hold the push. May indicate **upcoming bullish reversal**.

---

#### 4. Bullish Engulfing (Bullish)

```
  │
 ─┤  ← Previous candle (bearish, small body)
  │
  │
 ┌──┐
 │██│ ← Current candle (bullish, large body)
 │██│    Engulfs the previous body completely
 └──┘
```

Two-candle pattern. Current bullish candle completely engulfs the previous bearish candle. Strong signal of **bullish reversal**.

**Detection:** Previous candle is bearish (Close < Open), current is bullish (Close > Open), current body > previous body.

---

#### 5. Bearish Engulfing (Bearish)

```
 ┌──┐
 │  │ ← Previous candle (bullish, small body)
 └──┘
 ┌──┐
 │██│ ← Current candle (bearish, large body)
 │██│    Engulfs the previous body completely
 └──┘
```

Opposite of Bullish Engulfing. Current bearish candle engulfs previous bullish candle. Strong signal of **bearish reversal**.

---

#### 6. Piercing Line (Bullish)

```
  │
 ─┤  ← Previous bearish candle
  │
 ┌──┐
 │  │ ← Current candle opens below prev low
 └──┘    but closes above midpoint of prev body
```

Two-candle pattern. Previous candle is bearish. Current candle opens below the previous close but closes above the midpoint of the previous body. Bullish reversal signal.

---

#### 7. Dark Cloud Cover (Bearish)

```
 ┌──┐
 │  │ ← Previous bullish candle
 └──┘
  │
 ─┤  ← Current candle opens above prev high
  │     but closes below midpoint of prev body
```

Opposite of Piercing Line. Bearish reversal signal after an uptrend.

---

#### 8. Morning Star (Bullish, 3 candles)

```
 ┌──┐
 │  │ ← 1. Bearish candle
 └──┘
  ─   ← 2. Small body (indecision)
 ┌──┐
 │██│ ← 3. Bullish candle (closes above midpoint of candle 1)
 └──┘
```

Three-candle reversal pattern. First candle is bearish, second is a small-body candle (indecision), third is a strong bullish candle. Classic **bullish reversal** signal.

---

#### 9. Evening Star (Bearish, 3 candles)

```
  ┌──┐
  │██│ ← 1. Bullish candle
  └──┘
   ─   ← 2. Small body (indecision)
  ┌──┐
  │  │ ← 3. Bearish candle (closes below midpoint of candle 1)
  └──┘
```

Opposite of Morning Star. Bearish reversal signal.

---

#### 10. Three White Soldiers (Bullish)

```
  ┌──┐
  │██│
  └──┘
    ┌──┐
    │██│ ← Ascending closes
    └──┘
      ┌──┐
      │██│
      └──┘
```

Three consecutive bullish candles, each closing higher than the previous. Strong bullish signal indicating **sustained buying pressure**.

---

#### 11. Three Black Crows (Bearish)

```
 ┌──┐
 │  │
 └──┘
   ┌──┐
   │  │ ← Descending closes
   └──┘
     ┌──┐
     │  │
     └──┘
```

Opposite of Three White Soldiers. Three consecutive bearish candles. Strong bearish signal.

---

#### 12. Harami Cross (Neutral)

```
 ┌──┐
 │██│ ← Previous candle (large body)
 └──┘
  ─   ← Current candle: Doji inside previous body
```

A Doji candle that appears inside the body of the previous larger candle. Signals **indecision and potential reversal**. Direction depends on the trend context.

---

#### 13. Bearish Harami (Bearish)

```
 ┌──┐
 │  │ ← Previous bullish candle (large body)
 └──┘
  ┌─┐
  │ │ ← Current candle: small body inside prev body
  └─┘
```

Current candle's body is completely inside the previous candle's body. After an uptrend, indicates **bearish reversal**.

---

## ML Model

- **Algorithm:** GradientBoosting (200 trees, depth=5, lr=0.1)
- **Features:** 13 patterns + returns + volatility + volume_ratio
- **Target:** next candle > current close (1=LONG, 0=SHORT)
- **Metrics:** Accuracy, Precision, Recall, F1, ROC-AUC

### Feature Engineering

1. **Candlestick patterns** — 13 binary features (True/False for each pattern)
2. **Returns** — price change percentage
3. **Volatility** — standard deviation of recent prices
4. **Volume ratio** — current volume vs average volume

### Stationarity

All features are tested for stationarity using the **Augmented Dickey-Fuller (ADF) test**. Non-stationary features are transformed via:
- Log transformation
- Log + differencing
- First differencing

This ensures the ML model trains on statistically meaningful data.

---

## Project Structure

| File | Description |
|------|-------------|
| `main.py` | Bot entry point — loads data, runs strategy, generates signals |
| `strategy.py` | Strategy logic — patterns + ML model combined |
| `data_loader.py` | Binance klines data loader |
| `patterns.py` | Candlestick pattern detection (13 patterns) |
| `ml_models.py` | GradientBoosting model training and prediction |
| `stationarity.py` | ADF test and stationarity transformation |
| `backtest.py` | Backtester with SL/TP and metrics |
| `ML_Trading_Bot.ipynb` | [**Colab notebook**](ML_Trading_Bot.ipynb) — full source with theory and visualizations |
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
