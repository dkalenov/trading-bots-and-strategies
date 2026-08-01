"""
Backtest engine with stop-loss, take-profit and risk-based position
sizing.

READ THIS BEFORE TRUSTING ANY NUMBER THIS PRINTS.
===================================================
No backtest built from OHLC candles can be "100% accurate" — that is
a hard data limitation, not a bug to fix:
  - Candles only give you open/high/low/close, not the actual path
    price took inside the bar. If both the stop and the target were
    inside a bar's high-low range, there is no way to know from the
    candle which was hit first.
  - Real fills involve slippage and partial fills that depend on
    live order-book depth, which historical candles don't contain.
  - Funding fees (perpetual futures), exchange-specific liquidation
    mechanics, and latency are not modeled here.

What this engine DOES do is make its assumptions explicit and lean
conservative rather than optimistic wherever there's ambiguity:
  - Signals are executed at the NEXT bar's open, never the bar that
    produced them (no look-ahead).
  - If a bar's range touches both the stop and the target, the stop
    is assumed to have been hit first (worse case for the trader).
  - If a bar's open already gapped through the stop, the fill is at
    that open price, not at the stop price (gap risk is not hidden).
  - Take-profit fills are modeled as a resting limit order — filled
    at the target price, never assumed better.

Position sizing:
  Fixed-fractional risk sizing. You choose `risk_per_trade` (fraction
  of current equity you're willing to lose if the stop is hit). The
  position size is then back-solved from the distance to the stop, so
  a tight stop means a smaller position and a wide stop means a
  larger one — the dollar risk per trade stays constant. Size is
  capped by `max_leverage` (fraction of equity as notional exposure)
  so a very tight/noisy stop can't imply an absurd, unrealistic
  position.

Stop-loss placement (`stop_mode`):
  - "structure" (default): the swing point that defined the setup —
    for a LONG, the higher-low; for a SHORT, the lower-high. This is
    the level at which the pattern that generated the signal is
    itself invalidated, which is a natural place to be proven wrong.
  - "atr": entry price -/+ `atr_multiplier` * ATR(`atr_period`).
  - "percent": entry price -/+ a fixed `stop_pct`.

Take-profit is placed at `reward_risk_ratio` times the stop distance
(e.g. 2.0 = aim for 2x the risk taken).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from indicators import compute_atr


@dataclass
class Trade:
    direction: int  # 1 = long, -1 = short
    entry_idx: int
    entry_date: object
    entry_price: float
    entry_equity: float
    stop_price: float
    take_profit_price: float
    position_fraction: float  # fraction of equity used as notional exposure
    exit_idx: int | None = None
    exit_date: object | None = None
    exit_price: float | None = None
    exit_equity: float | None = None
    exit_reason: str | None = None  # "stop_loss" | "take_profit" | "signal_flip" | "end_of_data"
    status: str = "open"  # "open" or "closed"
    pnl: float = 0.0
    return_pct: float = 0.0


@dataclass
class BacktestResult:
    trades: list[Trade] = field(default_factory=list)
    equity_curve: pd.Series = None
    metrics: dict = field(default_factory=dict)

    def trades_df(self) -> pd.DataFrame:
        rows = []
        for t in self.trades:
            rows.append(
                {
                    "direction": "LONG" if t.direction == 1 else "SHORT",
                    "entry_date": t.entry_date,
                    "entry_price": t.entry_price,
                    "stop_price": t.stop_price,
                    "take_profit_price": t.take_profit_price,
                    "position_fraction": t.position_fraction,
                    "exit_date": t.exit_date,
                    "exit_price": t.exit_price,
                    "exit_reason": t.exit_reason,
                    "status": t.status,
                    "pnl": t.pnl,
                    "return_pct": t.return_pct,
                }
            )
        return pd.DataFrame(rows)


def _resolve_stop_price(
    direction: int,
    entry_price: float,
    structure_stop: float | None,
    stop_mode: str,
    atr_value: float | None,
    atr_multiplier: float,
    stop_pct: float,
    min_stop_pct: float,
    max_stop_pct: float,
) -> float | None:
    """Return a stop price for this entry, or None if it can't be determined."""
    stop_price = None

    if stop_mode == "structure" and structure_stop is not None and not np.isnan(structure_stop):
        stop_price = structure_stop
    elif stop_mode == "atr" and atr_value is not None and not np.isnan(atr_value) and atr_value > 0:
        stop_price = entry_price - direction * atr_multiplier * atr_value
    elif stop_mode == "percent":
        stop_price = entry_price * (1 - direction * stop_pct)

    # Fallback chain: structure -> atr -> percent, so a trade is never
    # skipped just because one particular stop input was unavailable.
    if stop_price is None:
        if atr_value is not None and not np.isnan(atr_value) and atr_value > 0:
            stop_price = entry_price - direction * atr_multiplier * atr_value
        else:
            stop_price = entry_price * (1 - direction * stop_pct)

    # Sanity-clip the stop distance so a near-zero or absurdly wide
    # structural stop can't blow up position sizing.
    dist_pct = abs(entry_price - stop_price) / entry_price
    dist_pct = min(max(dist_pct, min_stop_pct), max_stop_pct)
    stop_price = entry_price - direction * dist_pct * entry_price
    return stop_price


def run_backtest(
    df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    fee_rate: float = 0.0004,
    periods_per_year: float = 24 * 365,
    risk_per_trade: float = 0.01,
    stop_mode: str = "structure",
    atr_period: int = 14,
    atr_multiplier: float = 1.5,
    stop_pct: float = 0.02,
    reward_risk_ratio: float = 2.0,
    max_leverage: float = 1.0,
    min_stop_pct: float = 0.001,
    max_stop_pct: float = 0.15,
) -> BacktestResult:
    """
    df must contain: date, open, high, low, close, signal, stop_level
    (i.e. the output of fractals.generate_signals).

    risk_per_trade   fraction of current equity risked if the stop is
                      hit, e.g. 0.01 = risk 1% of equity per trade.
    stop_mode         "structure" | "atr" | "percent" — see module docstring.
    reward_risk_ratio take-profit distance as a multiple of stop distance.
    max_leverage      cap on notional exposure as a fraction of equity
                      (1.0 = never more than 100% of equity at risk of
                      price movement, i.e. no leverage).
    min_stop_pct/max_stop_pct
                      sanity bounds clipping the stop distance so
                      sizing can't blow up on a near-zero stop or
                      become meaningless on a huge one.
    """
    if stop_mode not in ("structure", "atr", "percent"):
        raise ValueError("stop_mode must be 'structure', 'atr' or 'percent'")

    dates = df["date"].to_numpy()
    opens = df["open"].to_numpy()
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    closes = df["close"].to_numpy()
    signals = df["signal"].to_numpy()
    structure_stops = df["stop_level"].to_numpy() if "stop_level" in df.columns else np.full(len(df), np.nan)
    atr = compute_atr(df, period=atr_period).to_numpy()
    n = len(df)

    equity = np.full(n, np.nan)
    cash = initial_capital
    position: Trade | None = None
    trades: list[Trade] = []

    def close_position(i: int, fill_price: float, reason: str, status: str = "closed") -> None:
        nonlocal cash, position
        moved = (fill_price - position.entry_price) / position.entry_price * position.direction
        gross_equity = position.entry_equity * (1 + position.position_fraction * moved)
        exit_fee = gross_equity * position.position_fraction * fee_rate
        cash = gross_equity - exit_fee

        position.exit_idx = i
        position.exit_date = dates[i]
        position.exit_price = fill_price
        position.exit_equity = cash
        position.exit_reason = reason
        position.status = status
        position.return_pct = moved * 100
        position.pnl = cash - position.entry_equity
        trades.append(position)
        position = None

    for i in range(n):
        # --- A) execute a signal from the previous bar's close, at this bar's open
        if i > 0 and signals[i - 1] != 0:
            new_direction = int(signals[i - 1])

            if position is not None and position.direction != new_direction:
                close_position(i, opens[i], reason="signal_flip")

            if position is None:
                entry_price = opens[i]
                struct_stop = structure_stops[i - 1]
                atr_val = atr[i - 1] if i - 1 < len(atr) else np.nan

                stop_price = _resolve_stop_price(
                    new_direction, entry_price, struct_stop, stop_mode,
                    atr_val, atr_multiplier, stop_pct, min_stop_pct, max_stop_pct,
                )
                stop_dist = abs(entry_price - stop_price)
                take_profit_price = entry_price + new_direction * reward_risk_ratio * stop_dist

                stop_dist_pct = stop_dist / entry_price
                position_fraction = min(risk_per_trade / stop_dist_pct, max_leverage)

                entry_fee = cash * position_fraction * fee_rate
                entry_equity = cash - entry_fee
                cash = entry_equity

                position = Trade(
                    direction=new_direction,
                    entry_idx=i,
                    entry_date=dates[i],
                    entry_price=entry_price,
                    entry_equity=entry_equity,
                    stop_price=stop_price,
                    take_profit_price=take_profit_price,
                    position_fraction=position_fraction,
                )

        # --- B) intrabar stop-loss / take-profit check for the live position
        if position is not None:
            d = position.direction
            if d == 1:
                stop_hit = lows[i] <= position.stop_price
                tp_hit = highs[i] >= position.take_profit_price
            else:
                stop_hit = highs[i] >= position.stop_price
                tp_hit = lows[i] <= position.take_profit_price

            if stop_hit and tp_hit:
                # ambiguous within this bar — conservative assumption: stop first
                fill = opens[i] if (d == 1 and opens[i] <= position.stop_price) or \
                                    (d == -1 and opens[i] >= position.stop_price) else position.stop_price
                close_position(i, fill, reason="stop_loss (same-bar ambiguous, assumed worst case)")
            elif stop_hit:
                fill = opens[i] if (d == 1 and opens[i] <= position.stop_price) or \
                                    (d == -1 and opens[i] >= position.stop_price) else position.stop_price
                close_position(i, fill, reason="stop_loss")
            elif tp_hit:
                close_position(i, position.take_profit_price, reason="take_profit")

        # --- C) mark-to-market equity for this bar
        if position is None:
            equity[i] = cash
        else:
            moved = (closes[i] - position.entry_price) / position.entry_price * position.direction
            equity[i] = position.entry_equity * (1 + position.position_fraction * moved)

    if position is not None:
        close_position(n - 1, closes[-1], reason="end_of_data", status="open")

    equity_series = pd.Series(equity, index=df["date"])
    metrics = _compute_metrics(trades, equity_series, initial_capital, periods_per_year)
    return BacktestResult(trades=trades, equity_curve=equity_series, metrics=metrics)


def _compute_metrics(
    trades: list[Trade], equity: pd.Series, initial_capital: float, periods_per_year: float
) -> dict:
    equity = equity.ffill().fillna(initial_capital)
    total_return_pct = (equity.iloc[-1] / initial_capital - 1) * 100

    running_max = equity.cummax()
    drawdown = (equity - running_max) / running_max
    max_drawdown_pct = drawdown.min() * 100

    bar_returns = equity.pct_change().dropna()
    if bar_returns.std() > 0:
        sharpe = (bar_returns.mean() / bar_returns.std()) * np.sqrt(periods_per_year)
    else:
        sharpe = 0.0

    closed = [t for t in trades if t.status == "closed"]
    wins = [t for t in closed if t.return_pct > 0]
    losses = [t for t in closed if t.return_pct <= 0]
    win_rate = (len(wins) / len(closed) * 100) if closed else 0.0
    avg_win = np.mean([t.return_pct for t in wins]) if wins else 0.0
    avg_loss = np.mean([t.return_pct for t in losses]) if losses else 0.0
    gross_win = sum(t.return_pct for t in wins)
    gross_loss = abs(sum(t.return_pct for t in losses))
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else float("inf")

    stop_outs = sum(1 for t in closed if t.exit_reason and t.exit_reason.startswith("stop_loss"))
    take_profits = sum(1 for t in closed if t.exit_reason == "take_profit")
    signal_flips = sum(1 for t in closed if t.exit_reason == "signal_flip")

    return {
        "initial_capital": initial_capital,
        "final_equity": float(equity.iloc[-1]),
        "total_return_pct": float(total_return_pct),
        "max_drawdown_pct": float(max_drawdown_pct),
        "sharpe_ratio": float(sharpe),
        "num_trades": len(closed),
        "win_rate_pct": float(win_rate),
        "avg_win_pct": float(avg_win),
        "avg_loss_pct": float(avg_loss),
        "profit_factor": float(profit_factor),
        "stop_outs": stop_outs,
        "take_profits": take_profits,
        "signal_flips": signal_flips,
        "open_position_at_end": bool(trades) and trades[-1].status == "open",
    }


def print_report(result: BacktestResult) -> None:
    m = result.metrics
    print("=" * 55)
    print("BACKTEST REPORT")
    print("=" * 55)
    print(f"Initial capital      : {m['initial_capital']:,.2f}")
    print(f"Final equity         : {m['final_equity']:,.2f}")
    print(f"Total return         : {m['total_return_pct']:.2f}%")
    print(f"Max drawdown         : {m['max_drawdown_pct']:.2f}%")
    print(f"Sharpe ratio (ann.)  : {m['sharpe_ratio']:.2f}")
    print(f"Number of trades     : {m['num_trades']}")
    print(f"  - stopped out      : {m['stop_outs']}")
    print(f"  - take-profit hit  : {m['take_profits']}")
    print(f"  - closed on flip   : {m['signal_flips']}")
    print(f"Win rate             : {m['win_rate_pct']:.2f}%")
    print(f"Avg win / avg loss   : {m['avg_win_pct']:.2f}% / {m['avg_loss_pct']:.2f}%")
    print(f"Profit factor        : {m['profit_factor']:.2f}")
    if m["open_position_at_end"]:
        print("Note: last position was still open at the end of the data")
        print("      (marked-to-market, not a realized trade).")
    print("=" * 55)
    print("Reminder: same-bar stop/target conflicts are resolved by")
    print("assuming the stop was hit first (worst case) — see backtest.py")
    print("docstring for every execution assumption this report relies on.")
