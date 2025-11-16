The goal of this project is to design and implement a **market-neutral long/short hedging strategy** with a **beta coefficient close to 0**.  
This minimizes exposure to overall market volatility (especially Bitcoin’s dominance in crypto markets) while allowing us to profit from **relative value differences** between correlated assets.

This work builds upon my previous GitHub project — [**Cointegrated Pairs Trading Bot**](https://github.com/dkalenov/Cointegrated-Pairs-Trading-bot) — where I developed a Python-based trading bot that identifies and trades cointegrated cryptocurrency pairs using **statistical arbitrage** and **mean-reversion** logic.  
The current project extends that concept toward **beta-neutral portfolio management**.

---

**Why Cointegration**

**Cointegration-based approach** allows to identify assets that move together in the long term but diverge temporarily in the short term.  
These temporary deviations offer **statistically measurable mean-reversion opportunities**.

Unlike correlation, cointegration ensures a **stationary linear relationship** between assets — allowing robust entry/exit decisions based on **z-scores** and **half-life estimates**.  
This method naturally provides **hedging** since long and short positions offset each other, reducing exposure to overall market movements.



## ****Implementation Plan****





**1. Data Preparation**

- Load and clean historical **kline (candlestick)** data for top cryptocurrencies.  
- Ensure all symbols share a unified time index, removing missing or duplicate candles.

---

**2. Beta Analysis**

- Estimate how strongly each cryptocurrency depends on **Bitcoin (BTC)** using log returns.


---

**3. Cointegration Scanning**

- Apply the **Engle–Granger two-step test** to detect statistically significant, mean-reverting long-term relationships.  
- Evaluate all symbol pairs for cointegration.  
- Retain only pairs that meet strict selection criteria:

  - p-value < 0.05  
  - Half-life < 200 bars  
  - Pair beta vs BTC ≈ 0  

---

**4. Z-Score and Signal Generation**

Compute the rolling z-score of each pair’s spread:


**Define trading signals:**

- **Go Long:**  z ≤ −2  
- **Go Short:** z ≥ +2  
- **Exit:** |z| ≤ 0.5  

Entries occur at statistical extremes; exits near equilibrium.

---

**5. Pair-Level Beta Calculation**

Identify combinations of assets which joint spread shows minimal dependency on BTC (**target β ≈ 0**).

---

**6. Position Sizing and Risk Management**

Apply **volatility parity** to balance exposure between both legs:



**Limit exposure:**

- Max notional per pair = **5% of total capital**  
- Max risk per pair = **1% of total capital**  
- Maintain **portfolio beta near zero** relative to BTC.

---

**7. Backtesting and Evaluation**

Run backtests on **≥ 1.5 years of hourly data**.

Track performance metrics:

- **CAGR** (Compound Annual Growth Rate)  
- **MDD** (Maximum Drawdown)  
- **CAGR/MDD > 1.5**  
- **Sharpe ratio** and **beta vs BTC**

Save trade logs, equity curves, and performance reports.

---

**8. Optimization and Diversification**

- Evaluate sensitivity for **z-entry/z-exit thresholds**, **lookback windows**, and **volatility weighting**.  
- Combine multiple low-correlated cointegration models into a diversified **market-neutral portfolio**.  
- Periodically re-train and re-evaluate cointegration pairs to adapt to market changes.  
- Monitor overall **portfolio beta** to maintain neutrality.

---

**9. Conclusion and Next Steps**

- **Parameter optimization:** Tune z-score thresholds, lookback windows, and half-life using grid search or Hyperopt.  
- **Advanced backtesting:** Extend the tester to stream data window-by-window to simulate live conditions.  
- **Dynamic adaptation:** Detect changes in cointegration; exit trades if relationships decay.  
- **Multi-timeframe analysis:** Explore cross-timeframe cointegration opportunities.  
- **Trade management:** Test partial take-profits, trailing stops, and dynamic stop adjustments.  
- **Integration:** Connect to Binance Futures WebSocket for real-time paper trading, then transition to live execution.
