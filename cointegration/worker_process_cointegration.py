# Rolling Window Scanning

def worker_process(
    proc_id,
    pairs_list,
    npz_path,
    out_part_csv,
    WINDOW=200,
    STEP=50,
    MAX_HALF_LIFE=200,
    BETA_THRESHOLD=0.1,
    Z_ENTRY=2.0,
    CAPITAL=1_000_000,
    MAX_NOTIONAL_PER_PAIR=0.05,
    VOL_LOOKBACK=60,
    TEST_MODE=False,
    TEST_MAX_WINDOWS_PER_PAIR=30,
):
    """
    Scans all rolling windows for given pairs and finds tradable cointegrated relationships.
    Used inside parallel processes.
    """
    try:
        t0 = time.time()
        npz = np.load(npz_path, allow_pickle=True)
        symbols = [s.decode() if isinstance(s, bytes) else s for s in npz["symbols"]]
        dates = npz["dates"]
        logmat = npz["logmat"]
        shifts = npz["shifts"]
        sym2idx = {sym: i for i, sym in enumerate(symbols)}

        results_rows = []
        windows_done = 0

        for (a, b) in pairs_list:
            pairname = f"{a}-{b}"
            if a not in sym2idx or b not in sym2idx or "BTCUSDT" not in sym2idx:
                continue

            ia = sym2idx[a]
            ib = sym2idx[b]
            ibtc = sym2idx["BTCUSDT"]
            T = logmat.shape[0]
            max_start = T - WINDOW
            if max_start < 0:
                continue

            windows_for_pair = 0
            for start in range(0, max_start + 1, STEP):
                if TEST_MODE and windows_for_pair >= TEST_MAX_WINDOWS_PER_PAIR:
                    break
                end = start + WINDOW
                log1 = logmat[start:end, ia]
                log2 = logmat[start:end, ib]
                logbtc = logmat[start:end, ibtc]

                windows_done += 1
                windows_for_pair += 1

                # sanity checks
                if np.isnan(log1).any() or np.isnan(log2).any() or np.isnan(logbtc).any():
                    continue
                if np.allclose(log1, log1[0]) or np.allclose(log2, log2[0]):
                    continue

                # cointegration
                flag, hedge, hl, pval = calculate_cointegration(log1, log2)
                if flag != 1 or np.isnan(hl) or hl <= 0 or hl > MAX_HALF_LIFE:
                    continue

                # beta to BTC
                pr = np.diff(log1) - hedge * np.diff(log2)
                btr = np.diff(logbtc)
                beta_btc = calculate_pair_beta(pr, btr)
                if np.isnan(beta_btc) or abs(beta_btc) >= BETA_THRESHOLD:
                    continue

                # zscore and entry
                spread = log1 - hedge * log2
                z = calculate_z_last(spread)
                if np.isnan(z):
                    continue
                if z >= Z_ENTRY:
                    signal = -1
                elif z <= -Z_ENTRY:
                    signal = 1
                else:
                    continue

                # position sizing
                dollar1, dollar2 = vol_parity_notional(
                    log1, log2, hedge,
                    capital=CAPITAL,
                    max_notional_per_pair=MAX_NOTIONAL_PER_PAIR,
                    lookback=VOL_LOOKBACK
                )
                price1 = max(math.exp(log1[-1]) - shifts[ia], 1e-9)
                price2 = max(math.exp(log2[-1]) - shifts[ib], 1e-9)
                qty1, qty2 = calculate_qty(dollar1, dollar2, price1, price2, capital=CAPITAL)

                results_rows.append({
                    "pair": pairname,
                    "start_index": int(start),
                    "end_index": int(end),
                    "start_date": str(dates[start]),
                    "end_date": str(dates[end-1]),
                    "hedge_ratio": float(hedge),
                    "half_life": float(hl),
                    "p_value": float(pval),
                    "beta_btc": float(beta_btc),
                    "z": float(z),
                    "signal": int(signal),
                    "qty1": qty1,
                    "qty2": qty2,
                    "dollar1": round(dollar1, 2),
                    "dollar2": round(dollar2, 2),
                })

        # save result
        df_part = pd.DataFrame(results_rows)
        df_part.to_csv(out_part_csv, index=False)

    except Exception:
        traceback.print_exc()
        pd.DataFrame().to_csv(out_part_csv, index=False)