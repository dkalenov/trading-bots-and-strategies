import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint # Engle–Granger
from itertools import combinations


def calculate_cointegration(log1, log2, max_half_life=200):
    # Log-transform the series for variance stabilization
    # If there are zeros or negative values, shift the series up by |min|+1
    if np.any(log1 <= 0):
        log1 = log1 + abs(np.min(log1)) + 1
    if np.any(log2 <= 0):
        log2 = log2 + abs(np.min(log2)) + 1

    # Log-transform the series and calculate the cointegration test
    log1 = np.log(log1)
    log2 = np.log(log2)

    safe_p_value = np.nan
    try:
        # Engle–Granger cointegration test
        coint_t, p_value, crit_vals = coint(log1, log2)
        safe_p_value = float(p_value)

        # Calculate hedge ratio (beta) using OLS regression
        X = sm.add_constant(log2) # X: add a constant column to include the intercept (a) in the regressionThis allows OLS to fit both slope (b) and intercept

        model = sm.OLS(log1, X).fit() # Ordinary Least Squares.  delta = a + b * spread_lag + error. OLS finds the coefficients (a, b) that minimize the sum of squared errors
        hedge = float(model.params.iloc[1])

        # if np.isnan(hedge):
        #     return 0, np.nan, np.nan, safe_p_value

        spread = log1 - hedge * log2
        hl = calculate_half_life(spread)


        if np.isnan(hl) or hl <= 0 or hl > max_half_life:
            return 0, hedge, np.nan, safe_p_value

        t_check = coint_t < crit_vals[1] # 5% significance level

        flag = 1 if (safe_p_value < 0.05 and t_check) else 0
        return flag, hedge, hl, safe_p_value




    except Exception as e:
        print(f"Colculate cointegration error: {e}")
        return 0, np.nan, np.nan, safe_p_value







def calculate_half_life(spread):
    """
    spread: numpy array or pandas Series (log-spread).
    Returns half-life in bars or np.nan if not mean-reverting or fail.
    """
    try:
        s = pd.Series(spread).dropna()
        if len(s) < 10:
            return np.nan

        # Δs = s_t - s_{t-1}, lagged_s = s_{t-1}
        spread_lag = s.shift(1).iloc[1:]
        delta = (s - s.shift(1)).iloc[1:]

        X = sm.add_constant(spread_lag)
        model = sm.OLS(delta, X).fit()
        b = float(model.params.iloc[1]) # slope coefficient (b) from the regression. This tells us how strongly the change in spread (delta) depends on the previous spread value
        if np.isnan(b):
            return np.nan
        phi = 1.0 + b
        if phi <= 0 or phi >= 1:
            return np.nan
        hl = -np.log(2) / np.log(phi)
        return float(round(hl, 2))


    except Exception as e:
        print(f"Half-life Calculate error: {e}")

def calculate_z_last(spread):
    """calculate zscore for last bar of spread window: (last - mean)/std"""
    s = pd.Series(spread)
    m = s.mean()
    sd = s.std()
    if sd == 0 or np.isnan(sd):
        return np.nan
    return float((s.iloc[-1] - m) / sd)



def calculate_pair_beta(pair_r, market_r):
    """ Beta = Cov(pair, market) / Var(market)"""

    if pair_r is None or market_r is None:
        return np.nan
    pair_r = np.array(pair_r, dtype=float)
    market_r = np.array(market_r, dtype=float)
    if len(pair_r) != len(market_r) or len(pair_r) < 5:
        return np.nan
    cov = np.cov(pair_r, market_r)[0, 1] # сovariance between pair and market returns
    var_m = np.var(market_r)  # variance of market returns (spread of market movement)
    if var_m == 0: # if variance = 0, market didn't move -> undefined beta
        return np.nan
    return float(cov / var_m)



if __name__ == "__main__":
    # Load and prepare data
    df = pd.read_csv("klines_data_1h_clean_100symbols.csv")
    df_pivot = df.pivot_table(index="Date", columns="Symbol", values="Close")

    symbols = df_pivot.columns[:10]  # first 10 symbols for a quick test
    print(f"First 3 symbols: {list(symbols)}")

    # Compute BTC returns for beta
    if "BTCUSDT" in df_pivot.columns:
        market_returns = df_pivot["BTCUSDT"].pct_change().dropna().values
    else:
        market_returns = None

    for s1, s2 in combinations(symbols, 2):
        pair_df = pd.concat([df_pivot[s1], df_pivot[s2]], axis=1).dropna()
        asset1, asset2 = pair_df[s1], pair_df[s2]

        try:
            # 1. Cointegration test
            flag, hedge, hl, p = calculate_cointegration(asset1, asset2)
            print(f"\n{s1} | {s2}: cointegration = {'YES' if flag else 'NO'} | hedge={hedge:.4f} | half-life={hl} | p={p:.5f}")

            # 2. Z-score test (spread)
            spread = np.log(asset1 + abs(asset1.min()) + 1) - hedge * np.log(asset2 + abs(asset2.min()) + 1)
            z_last = calculate_z_last(spread)
            print(f"Z-score (last): {z_last:.3f}")

            # 3. Beta vs BTC
            if market_returns is not None:
                pair_returns = pair_df.mean(axis=1).pct_change().dropna().values
                aligned_len = min(len(pair_returns), len(market_returns))
                beta = calculate_pair_beta(pair_returns[-aligned_len:], market_returns[-aligned_len:])
                print(f"Beta vs BTCUSDT: {beta:.3f}")
            else:
                print("Beta vs BTCUSDT: N/A (BTC not found)")

        except Exception as e:
            print(f"Calculation error for {s1}-{s2}: {e}")

