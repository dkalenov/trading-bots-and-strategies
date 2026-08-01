"""Chart plotting: candlesticks with signals, and the equity curve."""

from __future__ import annotations

import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def plot_price_with_signals(
    df: pd.DataFrame,
    symbol: str,
    interval: str,
    out_path: str,
    max_bars: int | None = 1000,
    trades_df: pd.DataFrame | None = None,
) -> str:
    """
    Plot OHLC candles with LONG/SHORT entry markers and save to out_path.
    If the frame is longer than max_bars, only the last max_bars candles
    are drawn (a full multi-year 1h/5m chart is unreadable anyway).

    If trades_df is given (backtest.BacktestResult.trades_df()), exits
    are overlaid too: X = stopped out, star = take-profit, diamond =
    closed on an opposite signal.
    """
    d = df.copy()
    if max_bars is not None and len(d) > max_bars:
        d = d.iloc[-max_bars:]
    window_start, window_end = d["date"].iloc[0], d["date"].iloc[-1]

    fig, ax = plt.subplots(figsize=(16, 8))

    for _, row in d.iterrows():
        color = "green" if row["close"] >= row["open"] else "red"
        ax.plot([row["date"], row["date"]], [row["low"], row["high"]], color="black", linewidth=0.8)
        ax.plot([row["date"], row["date"]], [row["open"], row["close"]], color=color, linewidth=4)

    longs = d[d["signal"] == 1]
    shorts = d[d["signal"] == -1]
    ax.scatter(longs["date"], longs["low"] * 0.999, color="lime", marker="^", s=120,
               label="LONG entry", zorder=5, edgecolors="black")
    ax.scatter(shorts["date"], shorts["high"] * 1.001, color="red", marker="v", s=120,
               label="SHORT entry", zorder=5, edgecolors="black")

    if trades_df is not None and len(trades_df) > 0:
        t = trades_df.copy()
        t["exit_date"] = pd.to_datetime(t["exit_date"])
        t = t[(t["exit_date"] >= window_start) & (t["exit_date"] <= window_end)]

        stopped = t[t["exit_reason"].astype(str).str.startswith("stop_loss")]
        tp = t[t["exit_reason"] == "take_profit"]
        flipped = t[t["exit_reason"] == "signal_flip"]

        ax.scatter(stopped["exit_date"], stopped["exit_price"], color="black", marker="x", s=90,
                   label="Stopped out", zorder=6)
        ax.scatter(tp["exit_date"], tp["exit_price"], color="gold", marker="*", s=180,
                   label="Take-profit", zorder=6, edgecolors="black")
        ax.scatter(flipped["exit_date"], flipped["exit_price"], color="dodgerblue", marker="D", s=60,
                   label="Closed on flip", zorder=6, edgecolors="black")

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b\n%H:%M"))
    plt.xticks(rotation=30)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.title(f"{symbol} {interval} — fractal breakout signals")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend(loc="upper left")
    plt.tight_layout()

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def plot_equity_curve(equity: pd.Series, out_path: str) -> str:
    """Plot the backtest equity curve and save to out_path."""
    fig, ax = plt.subplots(figsize=(16, 6))
    ax.plot(equity.index, equity.values, color="steelblue", linewidth=1.5)
    ax.axhline(equity.iloc[0], color="gray", linestyle="--", linewidth=1, alpha=0.7)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.title("Equity curve")
    plt.xlabel("Date")
    plt.ylabel("Account equity")
    plt.tight_layout()

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path
