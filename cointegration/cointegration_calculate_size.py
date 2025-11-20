import numpy as np



def vol_parity_notional(log1, log2, hedge,  capital=1_000_000, max_notional_per_pair= 0.05, lookback=60):
    """Return dollar allocations for legs based on inverse realized vol (log returns)"""

    cap_pair_usd = capital * max_notional_per_pair
    r1 = np.diff(log1[-lookback:]) if len(log1) >= lookback else np.diff(log1)
    r2 = np.diff(log2[-lookback:]) if len(log2) >= lookback else np.diff(log2)
    sigma1 = np.std(r1) if len(r1) > 0 else 0.0
    sigma2 = np.std(r2) if len(r2) > 0 else 0.0
    w1_raw = 1.0 / sigma1 if sigma1 > 0 else 0.0
    w2_raw = abs(hedge) / sigma2 if sigma2 > 0 else 0.0
    W = w1_raw + w2_raw
    if W <= 0:
        return 0.0, 0.0
    w1 = w1_raw / W
    w2 = w2_raw / W
    return float(cap_pair_usd * w1), float(cap_pair_usd * w2)


def calculate_qty(dollar1, dollar2, price1, price2, capital=1_000_000, max_notional_per_pair= 0.05):
    """Convert dollars to qty and enforce max notional"""
    max_notional = capital * max_notional_per_pair
    tot = abs(dollar1) + abs(dollar2)
    if tot > max_notional and tot > 0:
        scale = max_notional / tot
        dollar1 *= scale
        dollar2 *= scale
    qty1 = dollar1 / price1 if price1 > 0 else 0.0
    qty2 = dollar2 / price2 if price2 > 0 else 0.0
    return float(qty1), float(qty2)

