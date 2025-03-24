import numpy as np

def find_williams_fractals(df):
    """
    Identifies Williams fractals and generates signals for long and short positions.

    :param df: DataFrame with columns ['date', 'high', 'low']
    :return: DataFrame with added fractal and signal columns
    """

    # Create necessary columns
    df = df.copy()  # Avoid modifying the original DataFrame
    df["max1"] = np.nan
    df["min1"] = np.nan
    df["max2"] = np.nan
    df["min2"] = np.nan
    df["max1_date"] = np.nan
    df["min1_date"] = np.nan
    df["max2_date"] = np.nan
    df["min2_date"] = np.nan
    df["signal"] = 0  # 1 = Long, -1 = Short

    # Variables to track fractals
    last_max1 = last_min1 = last_max2 = last_min2 = None
    date_last_max1 = date_last_min1 = date_last_max2 = date_last_min2 = None

    for i in range(2, len(df) - 2):
        # Identify a local high (upward fractal)
        if df.iloc[i]["high"] > df.iloc[i - 1]["high"] and df.iloc[i]["high"] > df.iloc[i + 1]["high"] and \
           df.iloc[i]["high"] > df.iloc[i - 2]["high"] and df.iloc[i]["high"] > df.iloc[i + 2]["high"]:

            if last_max1 is None:
                last_max1 = df.iloc[i]["high"]
                date_last_max1 = df.iloc[i]["date"]
                df.loc[df.index[i], "max1"] = last_max1
                df.loc[df.index[i], "max1_date"] = date_last_max1
            elif last_max2 is None and last_min1 is not None:
                last_max2 = df.iloc[i]["high"]
                date_last_max2 = df.iloc[i]["date"]
                df.loc[df.index[i], "max2"] = last_max2
                df.loc[df.index[i], "max2_date"] = date_last_max2

                # Reset if trend conditions are violated
                if last_max1 < last_max2:
                    last_max1 = last_min1 = last_max2 = last_min2 = None
                    date_last_max1 = date_last_min1 = date_last_max2 = date_last_min2 = None
                elif last_max2 < last_min1:
                    last_max1 = last_max2
                    date_last_max1 = date_last_max2
                    last_min1 = last_max2 = None
                    date_last_min1 = date_last_max2 = None

        # Identify a local low (downward fractal)
        if df.iloc[i]["low"] < df.iloc[i - 1]["low"] and df.iloc[i]["low"] < df.iloc[i + 1]["low"] and \
           df.iloc[i]["low"] < df.iloc[i - 2]["low"] and df.iloc[i]["low"] < df.iloc[i + 2]["low"]:

            if last_max1 is not None and last_min1 is None:
                last_min1 = df.iloc[i]["low"]
                date_last_min1 = df.iloc[i]["date"]
                df.loc[df.index[i], "min1"] = last_min1
                df.loc[df.index[i], "min1_date"] = date_last_min1
            elif last_max2 is not None and last_min1 is not None:
                last_min2 = df.iloc[i]["low"]
                date_last_min2 = df.iloc[i]["date"]
                df.loc[df.index[i], "min2"] = last_min2
                df.loc[df.index[i], "min2_date"] = date_last_min2

                # Reset if trend conditions are violated
                if last_min1 > last_min2:
                    last_max1 = last_min1 = last_max2 = last_min2 = None
                    date_last_max1 = date_last_min1 = date_last_max2 = date_last_min2 = None
                elif last_min2 is not None and last_max1 is not None and last_min2 > last_max1:
                    last_min1 = last_min2
                    date_last_min1 = date_last_min2
                    last_max1 = last_min2 = None
                    date_last_max1 = date_last_min2 = None

        # Check for a LONG (buy) signal
        if last_max1 is not None and last_min1 is not None and last_max2 is not None:
            if df.iloc[i]["low"] < last_min1:
                df.loc[df.index[i], "signal"] = 1  # Long signal
                df.loc[df.index[i], "min1"] = last_min1
                df.loc[df.index[i], "max1"] = last_max1
                df.loc[df.index[i], "max2"] = last_max2
                df.loc[df.index[i], "min1_date"] = date_last_min1
                df.loc[df.index[i], "max1_date"] = date_last_max1
                df.loc[df.index[i], "max2_date"] = date_last_max2
                last_max1 = last_min1 = last_max2 = last_min2 = None

        # Check for a SHORT (sell) signal
        if last_max1 is not None and last_min1 is not None and last_min2 is not None:
            if df.iloc[i]["high"] > last_max1:
                df.loc[df.index[i], "signal"] = -1  # Short signal
                df.loc[df.index[i], "min1"] = last_min1
                df.loc[df.index[i], "max1"] = last_max1
                df.loc[df.index[i], "min2"] = last_min2
                df.loc[df.index[i], "min1_date"] = date_last_min1
                df.loc[df.index[i], "max1_date"] = date_last_max1
                df.loc[df.index[i], "min2_date"] = date_last_min2
                last_max1 = last_min1 = last_max2 = last_min2 = None

    return df
