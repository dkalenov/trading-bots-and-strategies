"""
Backtest engine. Every trading decision here goes through core.strategy /
core.atr - the exact same functions the live bot uses. Nothing about
entries, exits, sizing, or the ATR is reimplemented in this file.

Known, disclosed approximation: we only have 4h OHLC, not tick data. The
live bot watches every tick and every mark-price update; here, when a stop
and a take level both fall inside the same candle's [Low, High], we cannot
tell from OHLC which was touched first, so we resolve the stop first. This
is a deliberate, conservative choice, documented here and in README.md -
it is not a bug and not hidden.
"""
from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from dataclasses import dataclass

from core.atr import wilder_atr
from core.strategy import decide_entry, compute_exit_levels, breakeven_stop_price, StrategyConfig, LONG


@dataclass
class BacktestConfig:
    strategy: StrategyConfig = None
    commission_rate: float = 0.0004   # Binance USDM taker fee, one side (documented assumption)
    slippage_rate: float = 0.0001     # documented assumption, applied on exits only
    signal_clean_start: str = "2025-03-02 00:00:00+00:00"  # see README: log is noisy before this

    def __post_init__(self):
        if self.strategy is None:
            self.strategy = StrategyConfig()


def load_klines(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["Date"] = pd.to_datetime(df["Date"], utc=True)
    return df.sort_values(["Symbol", "Date"]).reset_index(drop=True)


def load_signals(path: str, clean_start: str) -> pd.DataFrame:
    sig = pd.read_csv(path)
    sig["utc_time"] = pd.to_datetime(sig["utc_time"], utc=True, format="ISO8601")
    sig["t4h"] = sig["utc_time"].dt.floor("4h")
    sig = sig[sig["t4h"] >= pd.Timestamp(clean_start)].copy()
    sig = sig.sort_values("utc_time").drop_duplicates(subset=["symbol", "t4h"], keep="last")
    return sig


def run_backtest(klines: pd.DataFrame, signals: pd.DataFrame, cfg: BacktestConfig = None):
    cfg = cfg or BacktestConfig()
    scfg = cfg.strategy

    btc_sig = signals[signals["symbol"] == "BTCUSDT"].set_index("t4h")["signal"].to_dict()

    trades = []
    open_at_end = []
    symbols = sorted(set(klines["Symbol"].unique()) & set(signals["symbol"].unique()))

    for symbol in symbols:
        sdf = klines[klines["Symbol"] == symbol].reset_index(drop=True)
        if len(sdf) < scfg.atr_length + 5:
            continue
        sdf["ATR"] = wilder_atr(sdf, scfg.atr_length)
        sym_signals = signals[signals["symbol"] == symbol].set_index("t4h")["signal"].to_dict()

        position = None
        i = 0
        n = len(sdf)
        while i < n:
            row = sdf.iloc[i]
            t = row["Date"]

            if position is None:
                sig = sym_signals.get(t)
                if not pd.isna(row["ATR"]) and sig in ("STRONG_BUY", "STRONG_SELL"):
                    direction = decide_entry(sig, btc_sig.get(t))
                    if direction:
                        entry_price = row["Close"]
                        levels = compute_exit_levels(direction, entry_price, row["ATR"], scfg)
                        qty = scfg.order_size_usd / entry_price
                        position = dict(
                            symbol=symbol, direction=direction, entry_time=t,
                            entry_price=entry_price, atr=row["ATR"],
                            stop=levels.stop, take1=levels.take1, take2=levels.take2,
                            qty_full=qty, qty_remaining=qty, take1_done=False,
                            breakeven_stop=None, entry_idx=i,
                            realized_usd=0.0,
                            realized_fees_usd=scfg.order_size_usd * cfg.commission_rate,
                        )
                i += 1
                continue

            if i <= position["entry_idx"]:
                i = position["entry_idx"] + 1
                if i >= n:
                    break
                row = sdf.iloc[i]

            hi, lo = row["High"], row["Low"]
            long = position["direction"] == LONG

            if not position["take1_done"]:
                stop_hit = (lo <= position["stop"]) if long else (hi >= position["stop"])
                take1_hit = (hi >= position["take1"]) if long else (lo <= position["take1"])
                if stop_hit:
                    _close_full(position, position["stop"], cfg, trades, "STOP")
                    position = None
                elif take1_hit:
                    _partial_close_take1(position, scfg, cfg)
                    i += 1
                    continue
                else:
                    i += 1
                    continue
            else:
                be = position["breakeven_stop"]
                stop_hit = (lo <= be) if long else (hi >= be)
                take2_hit = (hi >= position["take2"]) if long else (lo <= position["take2"])
                if stop_hit:
                    _close_full(position, be, cfg, trades, "BREAKEVEN_STOP")
                    position = None
                elif take2_hit:
                    _close_full(position, position["take2"], cfg, trades, "TAKE2")
                    position = None
                else:
                    i += 1
                    continue
            i += 1

        if position is not None:
            open_at_end.append(position)

    return pd.DataFrame(trades), open_at_end


def _partial_close_take1(position, scfg: StrategyConfig, cfg: BacktestConfig):
    long = position["direction"] == LONG
    close_qty = position["qty_full"] * scfg.take1_portion
    entry, take1 = position["entry_price"], position["take1"]
    gross = (take1 - entry) * close_qty if long else (entry - take1) * close_qty
    position["realized_usd"] += gross
    position["realized_fees_usd"] += close_qty * take1 * cfg.commission_rate
    position["qty_remaining"] -= close_qty
    position["take1_done"] = True
    position["breakeven_stop"] = breakeven_stop_price(position["direction"], entry, scfg)


def _close_full(position, exit_price, cfg: BacktestConfig, trades: list, exit_reason: str):
    long = position["direction"] == LONG
    entry, qty = position["entry_price"], position["qty_remaining"]
    gross = (exit_price - entry) * qty if long else (entry - exit_price) * qty
    fee = qty * exit_price * (cfg.commission_rate + cfg.slippage_rate)
    total = position["realized_usd"] + gross - position["realized_fees_usd"] - fee
    trades.append(dict(
        symbol=position["symbol"], direction=position["direction"],
        entry_time=position["entry_time"], entry_price=entry,
        exit_reason=exit_reason, take1_done=position["take1_done"],
        pnl_usd=total, return_pct=total / (position["qty_full"] * entry) * 100.0,
    ))
