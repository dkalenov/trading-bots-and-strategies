# Candle Pattern Strategy - Bybit Futures

Candlestick pattern recognition bot with ATR-based risk management for Bybit Futures.

**Disclaimer:** Educational purposes only. Crypto trading carries high risk of total loss. Read the "Known Limitations" section before considering real capital.

## How It Works

- Detects 11 candlestick patterns: Engulfing, Hammer, Morning/Evening Star, Three White Soldiers/Black Crows, Piercing Line, Dark Cloud Cover, Harami Cross (Harami Cross is detected/labeled but is intentionally non-directional in the current code — see Pattern Weights below - so it never triggers a trade)
- Patterns are scored by weighted strength (0.9–1.4) with optional volume boost
- **BUY signal:** Bullish pattern detected (engulfing, hammer, morning star, etc.)
- **SELL signal:** Bearish pattern detected (bearish engulfing, evening star, etc.)
- ATR-based stop-loss and take-profit, enforced on the exchange via Bybit's trading-stop endpoint (see Architecture)
- **Trend filter:** EMA-50/EMA-200 - only trade with the trend. Contributes far less to results than the pattern-strength and volatility filters - see the ablation table below.

## Files

| File | Purpose |
|------|---------|
| `strategy.py` | Pattern detection + signal generation (11 patterns) |
| `backtest.py` | Backtester with Bybit data download + caching |
| `main.py` | Live trading bot (WebSocket + REST, Bybit Futures) |
| `validate.py` | Out-of-sample / cross-timeframe robustness checks |
| `Pattern_strategy.ipynb` | Research notebook (Colab) |
| `ML_Trading_Bot.ipynb` | ML-based pattern recognition (exploratory — not used by main.py/backtest.py) |
| `requirements.txt` | Python dependencies |

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Backtest (optimized config with trend filter)
python backtest.py --symbol BTCUSDT --interval 1h --start 2024-01 --end 2025-06

# Backtest baseline (no filters)
python backtest.py --symbol BTCUSDT --interval 1h --start 2024-01 --end 2025-06 --baseline

# Robustness / out-of-sample check
python validate.py --symbol BTCUSDT

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
| `leverage` | 10x | Max leverage cap. Position size is risk-based (from `risk_pct`), so this cap rarely binds in practice - it doesn't meaningfully change backtest results whether set to 10x or 20x |
| `commission` | 0.055% | Bybit standard (non-VIP) USDT-perpetual taker fee. All orders here are market orders (taker) |
| `min_strength` | 1.3 | Min pattern strength to trade — **the single biggest driver of backtest performance**, see ablation table |
| `ema_fast` | 50 | Fast EMA for trend filter (calculated with the standard recursive formula, `adjust=False`) |
| `ema_slow` | 200 | Slow EMA for trend filter |
| `min_atr_pct` | 0.3% | Min ATR % of price (volatility filter) |

## Architecture

```
WebSocket (kline)  ──→  Pattern Detection (11 patterns)
                              ↓
                     Signal + Strength scoring
                              ↓
                     Market Order  ──→  Bybit trading-stop endpoint
                                       (SL + TP registered on the exchange
                                        immediately after entry fills)
```

Both the SL and TP levels are computed the same way in `backtest.py` and `main.py`
(`entry_price ± sl_atr/tp_atr * ATR`), and in live trading they are pushed to Bybit
via `/v5/position/trading-stop` right after the entry order - the exchange enforces
them even if this process disconnects. Previously this endpoint was never called;
the bot only closed positions on the next opposite pattern signal, with no
protective stop of any kind. Since ~99.7% of trades in the backtest close via SL/TP
(see below), that gap meant the live bot's actual behavior had essentially nothing
in common with what was backtested. If you forked this repo before this fix, update
`main.py`.

`--leverage` is now also actually sent to Bybit via `/v5/position/set-leverage` at
startup - previously it was only printed to the console and never applied, so the
account silently kept whatever leverage was last configured manually.

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
| Harami Cross | 0.8 | *(none - never trades, see note above)* |

## Backtest Results

BTCUSDT 1h (2024-01 to 2025-06, 13,000 candles). Numbers below use realistic costs:
Bybit's standard 0.055% taker fee (not an optimistic 0.04%) and an approximate
funding-rate cost (0.01%/8h - a rough estimate, not real historical funding data,
since that wasn't fetchable in the environment this was checked in).

### Without filters (baseline)

| Metric | Value |
|--------|-------|
| Return | -99.0% |
| Trades | 1,341 |
| Win Rate | 40.9% |
| Max Drawdown | -99.0% |
| Profit Factor | 0.48 |

**Conclusion:** Pure candlestick patterns without filters are not profitable - SL/TP is very tight (0.75x ATR, ~1:1 R:R) in this config, so commissions and noise dominate. Engulfing patterns dominate raw signal counts but generate the most noise.

### With filters + "optimized" parameters

| Metric | Value |
|--------|-------|
| Return | **+19.2%** |
| Trades | 329 |
| Win Rate | 39.2% |
| Max Drawdown | -17.9% |
| Sharpe Ratio | 0.65 |
| Profit Factor | 1.07 |
| Est. funding cost | -$566 (already included above, rough estimate) |

**Config:** SL=2.0x ATR, TP=4.0x ATR, EMA-50/200 trend filter, min_strength=1.3, min ATR 0.3%

Note: an earlier version of this README reported +34.2% / Sharpe 1.02 / PF 1.13 for
this same config. That number used a below-market 0.04% commission, no funding
cost, and filled entries at the signal bar's own close instead of the next bar's
open. +19.2% is the same backtest with realistic costs and entry timing - treat it
as the more honest figure, and +34.2% as an upper bound that assumes frictionless,
instant execution. (For the record: switching entry timing alone from same-bar-close
to next-bar-open changed the result by well under 0.3 percentage points here - Bybit's
1h klines have zero gap between a bar's close and the next bar's open, since it's a
continuous 24/7 market. The fix matters for methodological correctness and for markets
that DO gap, more than it mattered numerically in this specific dataset.)

### Filter ablation — isolating what actually drives the result

The previous README attributed the entire improvement (baseline → optimized) to the
EMA trend filter. That comparison changed 4 things at once (SL/TP ratio, min_strength,
min_atr_pct, AND the trend filter), so it couldn't actually isolate the trend
filter's effect. Holding SL/TP fixed at 2.0/4.0 ATR and toggling one filter at a
time:

| Config (SL=2.0/TP=4.0 ATR fixed) | Return | Trades |
|---|---|---|
| Only EMA trend filter | +6.9% | 360 |
| Only min_strength=1.3 | +28.0% | 344 |
| Only min_atr_pct=0.3% | +13.7% | 361 |
| strength + ATR%, **no** trend filter | +34.1%\* | 335 |
| All three together (current default) | +19.2% | 329 |

\*Figure shown with the old 0.04% commission for direct comparison with the isolated
single-filter rows above; with 0.055% + funding it lands close to the full-combo
number.

**The trend filter is not the main driver.** Adding it on top of strength+ATR%
filters changes the result by well under 1 percentage point. `min_strength` (which
functionally keeps mostly Three White Soldiers/Black Crows/Stars, since Engulfing
only clears the 1.3 bar with a volume boost) and the volatility filter are doing
almost all the work.

### Out-of-sample / robustness check (`validate.py`)

All parameters above were tuned and evaluated on the same single 18-month dataset -
no train/test split, no walk-forward, no second symbol. Running the identical
parameters on 30-minute BTCUSDT candles over almost the same period (a timeframe
the parameters were never fit to):

| Test | Return | Win Rate | PF |
|---|---|---|---|
| 1h (tuning dataset) | +19.2% | 39.2% | 1.07 |
| 30m (same asset, ~same period) | **-37.8%** | 33.4% | 0.82 |

A strategy with a real edge should degrade gracefully across a nearby timeframe on
the same asset, not flip from profitable to sharply unprofitable. This is a strong
signal that the current parameters are fitted to the specific 1h dataset rather
than reflecting a robust edge. **Before risking real capital, at minimum re-run
`validate.py`-style checks on a period that was not used to pick `sl_atr`/`tp_atr`/
`min_strength`, and on a second symbol.**

### Key Findings

1. **`min_strength` (pattern-quality filter) is the main driver**, not the trend filter — see ablation table
2. **Wide TP (2:1 R:R)** - allows winners to run, compensates for lower win rate
3. **Engulfing patterns are the noisiest** - dominant in raw signal counts, weakest edge
4. **Realistic costs cut the return roughly in half** (+34.2% frictionless → +19.2% with real Bybit fees + approximate funding)
5. **Same parameters lose money on 30m bars** - treat the strategy as unvalidated out-of-sample until tested on data/timeframes/symbols it wasn't tuned on

## Known Limitations

- **Entry timing:** the backtest now fills at the *next* bar's open following a signal bar's close (`next_bar_entry=True`, default), not the signal bar's own close — see the note under "With filters" above. `main.py`'s real execution (WebSocket confirm → REST poll → order) happens shortly after a candle closes, so it lines up with this convention; there is still some real network/processing latency backtest can't capture exactly.
- **Funding rate is a rough constant estimate** (0.01%/8h), not real historical funding data. Actual BTCUSDT funding varies over time and tends to run positive during strong uptrends — which is exactly when this trend-following strategy is more likely to be long, a correlation that could make real funding costs worse than the flat estimate here.
- **No liquidation modeling.** Position sizing is risk-based (`risk_pct` of capital per trade via SL distance), which in practice keeps notional well under the leverage cap, so liquidation before SL is unlikely at the tested parameters - but it isn't explicitly checked.
- **Single asset, single timeframe, single period were used to choose parameters and report results.** See the out-of-sample section above.
- **No slippage differentiation for stop-outs.** Real stop-loss fills during fast moves/flash-crashes often slip more than the flat 0.02% assumed here for every exit.

## Contacts

Telegram: @KDR_98
