import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint


# === CONFIG ===
WINDOW = 100         # длина окна (баров)
STEP = 50            # шаг окна
MAX_HALF_LIFE = 100
INPUT_FILE = "klines_data_1h_simple.csv"
OUTPUT_FILE = "cointegrated_pairs_base_1h.csv"



# Calculate Half life
def calculate_half_life(spread):
  df_spread = pd.DataFrame(spread, columns=["spread"])
  spread_lag = df_spread.spread.shift(1)
  spread_lag.iloc[0] = spread_lag.iloc[1]
  spread_ret = df_spread.spread - spread_lag
  spread_ret.iloc[0] = spread_ret.iloc[1]
  spread_lag2 = sm.add_constant(spread_lag)
  model = sm.OLS(spread_ret, spread_lag2)
  res = model.fit()
  halflife = round(-np.log(2) / res.params.iloc[1], 0)
  return halflife



def calculate_zscore(spread):
  spread_series = pd.Series(spread)
  mean = spread_series.rolling(center=False, window=WINDOW).mean()
  std = spread_series.rolling(center=False, window=WINDOW).std()
  x = spread_series.rolling(center=False, window=1).mean()
  zscore = (x - mean) / std
  return zscore




def calculate_cointegration(series_1, series_2):
    """
    Проверка коинтеграции двух временных рядов с использованием теста Энгла–Грейнджера.
    Добавлено безопасное логарифмирование ценовых рядов для улучшения статистических свойств.

    Параметры
    ----------
    series_1 : pandas.Series или np.ndarray
        Первый временной ряд (например, цены актива A)
    series_2 : pandas.Series или np.ndarray
        Второй временной ряд (например, цены актива B)

    Возвращает
    ----------
    coint_flag : int
        1, если ряды коинтегрированы (p < 0.05 и t < критического значения), иначе 0
    hedge_ratio : float
        Коэффициент хеджирования β из регрессии OLS
    half_life : float
        Полураспад спреда (скорость возврата к среднему)
    p_value : float
        p-значение из теста коинтеграции
    """

    # --- Безопасное логарифмирование ---
    # Проверяем, что значения положительные, иначе логарифм будет некорректен
    # Если есть нули или отрицательные значения — сдвигаем ряд вверх на |min| + 1
    if np.any(series_1 <= 0):
        series_1 = series_1 + abs(np.min(series_1)) + 1
    if np.any(series_2 <= 0):
        series_2 = series_2 + abs(np.min(series_2)) + 1

    # Применяем натуральный логарифм
    log_series_1 = np.log(series_1)
    log_series_2 = np.log(series_2)

    # --- Тест Энгла–Грейнджера ---
    coint_res = coint(log_series_1, log_series_2)
    coint_t = coint_res[0]
    p_value = coint_res[1]
    critical_value = coint_res[2][1]  # 5% уровень значимости

    # --- Оценка коэффициента хеджирования через OLS ---
    model = sm.OLS(log_series_1, sm.add_constant(log_series_2)).fit()
    hedge_ratio = model.params[1]

    # --- Расчёт спреда и half-life ---
    spread = log_series_1 - (hedge_ratio * log_series_2)
    half_life = calculate_half_life(spread)

    # --- Проверка на коинтеграцию ---
    t_check = coint_t < critical_value
    coint_flag = 1 if p_value < 0.05 and t_check else 0

    return coint_flag, hedge_ratio, half_life, p_value






def store_cointegration_results_from_csv(file_path):
    print(f"📥 Loading data from {file_path}...")
    df = pd.read_csv(file_path)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Symbol", "Date"])
    df_pivot = df.pivot(index="Date", columns="Symbol", values="Close").dropna(axis=1)
    print(f"✅ Loaded {len(df_pivot.columns)} symbols")

    df_results = rolling_cointegration(df_pivot)

    if not df_results.empty:
        df_results.to_csv(OUTPUT_FILE, index=False)
        print(f"\n💾 Cointegrated pairs with time saved to {OUTPUT_FILE}")
        print(df_results.head())
    else:
        print("\n❌ No cointegrated pairs found.")






def rolling_cointegration(df_pivot):
    """Проверка коинтеграции во времени (rolling window)"""
    markets = df_pivot.columns.to_list()
    results = []

    for i, base_market in enumerate(markets[:-1]):
        for quote_market in markets[i + 1:]:
            for start in range(0, len(df_pivot) - WINDOW, STEP):
                end = start + WINDOW
                window_data = df_pivot.iloc[start:end]
                start_date = window_data.index[0]
                end_date = window_data.index[-1]

                s1 = window_data[base_market].values.astype(float)
                s2 = window_data[quote_market].values.astype(float)

                try:
                    coint_flag, hedge_ratio, half_life, p_value = calculate_cointegration(s1, s2)
                    if coint_flag == 1 and 0 < half_life <= MAX_HALF_LIFE:
                        results.append({
                            "start_date": start_date,
                            "end_date": end_date,
                            "base_market": base_market,
                            "quote_market": quote_market,
                            "hedge_ratio": hedge_ratio,
                            "half_life": half_life,
                            "p_value": round(p_value, 4)
                        })
                        print(f"✅ {base_market}-{quote_market} | {start_date.date()} → {end_date.date()} | HL={half_life} | p={p_value:.4f}")
                except Exception as e:
                    print(f"⚠️ Error {base_market}-{quote_market} @ {end_date}: {e}")

    return pd.DataFrame(results)
