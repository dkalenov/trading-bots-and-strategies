# Trading Bots & Strategies

Automated trading bots and quantitative strategies for cryptocurrency exchanges (Binance, Bybit).

---

## DISCLAIMER

> **All strategies and bots presented here are for EDUCATIONAL and RESEARCH purposes ONLY.**
>
> **These are NOT trading recommendations. Trading cryptocurrencies involves HIGH RISK. Using any data, code, or ideas from this repository may result in COMPLETE LOSS OF FUNDS.**
>
> - Past performance does not guarantee future results
> - The author is not responsible for any financial losses
> - Do not trade with money you cannot afford to lose
> - Do not run bots in live mode without fully understanding the risks

---

## About

19 trading strategies and bots covering:

- **Technical Analysis** — EMA, Bollinger Bands, SuperTrend, Williams Fractal, UTBot
- **Machine Learning** — candlestick pattern recognition
- **Statistical Arbitrage** — pairs trading (cointegration), mean reversion
- **Grid Strategies** — grid trading, density bot
- **Momentum** — spot and futures momentum
- **Alerts & Triggers** — price alerts, trigger orders
- **TradingView Signals** — indicator collection and backtesting

---

## Strategies

| Strategy | Type | Exchange | Backtest | Status |
|----------|------|----------|----------|--------|
| [EMA Bot](EMA_bot_bybit/) | Trend Following | Bybit Futures | [Yes](EMA_bot_bybit/backtest.py) | Working |
| [Bollinger Futures](boll_bybit_futures/) | Volatility | Bybit Futures | No | Working |
| [SuperTrend Bot](supertrend_bot_binance/) | Trend Following | Binance Futures | [Yes](supertrend_bot_binance/supertrend_backtest.ipynb) | Working |
| [Grid Trading](grid_trading_binance_futures/) | Grid | Binance Futures | No | Working |
| [Density Bot](density_bot_binance/) | Orderbook | Binance Futures | No | **Unprofitable** (HFT latency gap) |
| [UTBot Strategy](UTBot_Strategy/) | Trend | Research | [Yes](UTBot_Strategy/UTBot_backtest.ipynb) | Indicator |
| [Williams Fractal](williams_fractal_strategy/) | Pattern | Research | [Yes](williams_fractal_strategy/williams_fractal_strategy.ipynb) | Research |
| [Momentum Spot](momentum_binance_spot/) | Momentum | Binance Spot | No | Working |
| [Momentum Futures](momentum_futures_bot/) | Momentum | Binance Futures | [Yes](momentum_futures_bot/momentum_backtest/) | Advanced |
| [Cointegration](cointegration/) | Statistical Arbitrage | Research | No | Research |
| [Mean Reversion](mean_reversion_strategy/) | Statistical | Bybit Futures | No | Working |
| [Candle Patterns](candle_pattern_strategy/) | Pattern | Research | [Yes](candle_pattern_strategy/Pattern_strategy.ipynb) | Educational |
| [ML Candle Patterns](ML_candle_patterns_bot/) | ML | Research | [Yes](ML_candle_patterns_bot/ML_Trading_Bot.ipynb) | **Ineffective** |
| [Price Alerts](price_alerts_binance/) | Alerts | Binance | No | **Stub** |
| [Trigger Orders](trigger_orders_bot/) | Triggers | Binance Futures | No | Working |
| [Sloping Bot 2.0](sloping_bot_2.0/) | Trend | Binance Futures | [Yes](sloping_bot_2.0/sloping_backtest/) | Advanced |
| [Sloping Bot](sloping_bot_binance/) | Trend | Binance Futures | [Yes](sloping_bot_binance/Sloping_backtest.ipynb) | Previous version |
| [Breakout Spot](breakout_spot_binance/) | Breakout | Binance Spot | No | Working |
| [TradingView Screener](tradingview_screener/) | Signals | Binance Futures | [Yes](tradingview_screener/backtest/) | Working |

### Stats

- **Total strategies:** 19
- **With backtests:** 12
- **Without backtests:** 7
- **Exchanges:** Binance (11), Bybit (3), Research (5)

---

## Project Structure

```
trading-bots-and-strategies/
├── EMA_bot_bybit/                  EMA crossover, Bybit
├── ML_candle_patterns_bot/         ML candlestick patterns
├── UTBot_Strategy/                 UTBot indicator
├── boll_bybit_futures/             Bollinger Bands, Bybit
├── breakout_spot_binance/          Volume breakout, Binance
├── candle_pattern_strategy/        Candlestick patterns
├── cointegration/                  Pairs trading
├── density_bot_binance/            Orderbook density, Binance
├── grid_trading_binance_futures/   Grid trading, Binance
├── mean_reversion_strategy/        Mean reversion, Bybit
├── momentum_binance_spot/          Momentum, Binance Spot
├── momentum_futures_bot/           Momentum, Binance Futures
├── price_alerts_binance/           Stub
├── sloping_bot_2.0/                Sloping v2, Binance
├── sloping_bot_binance/            Sloping v1, Binance
├── supertrend_bot_binance/         SuperTrend + ADX, Binance
├── tradingview_screener/           TradingView signals, Binance
├── trigger_orders_bot/             S/R triggers, Binance
└── williams_fractal_strategy/      Williams Fractal
```

### Root-level files

| File | Description |
|------|-------------|
| `main.py` | Density Bot — main process (Binance Futures) |
| `ob.py` | Orderbook processor — WebSocket L2 depth + aggTrades |
| `backtest_by_month.py` | Hyperopt-powered monthly backtest framework |
| `func_cointegration_base_plus_improve.py` | Cointegration pair scanner (Engle-Granger) |
| `Sloping_backtest.ipynb` | Sloping strategy backtest |
| `backtest_results_1h_BTC.csv` | BTC 1h backtest results |
| `klines_data_30m.csv`, `klines_data_4h.csv` | Historical kline data |

---

## Tech Stack

- **Language:** Python 3.8+
- **Libraries:** pandas, numpy, ccxt, ta, scikit-learn, matplotlib, seaborn
- **ML:** PyTorch, Scikit-learn (GradientBoosting)
- **Exchanges:** Binance (ccxt), Bybit (API)
- **Notifications:** Telegram Bot API
- **Data:** Jupyter Notebook, CSV

---

## Installation

```bash
git clone https://github.com/dkalenov/trading-bots-and-strategies.git
cd trading-bots-and-strategies
pip install -r requirements.txt
```

For individual strategies, see `requirements.txt` in the strategy folder.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

MIT License — see [LICENSE](LICENSE).

---

## Contacts

- Telegram: [@KDR_98](https://t.me/KDR_98)
- LinkedIn: [dmitrii-kalenov](https://www.linkedin.com/in/dmitrii-kalenov)
- Email: drkalenov@gmail.com
