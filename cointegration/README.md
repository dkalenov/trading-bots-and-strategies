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
