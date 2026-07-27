"""
Shared grid-trading strategy core.

This module contains ONLY pure, deterministic logic — no network calls,
no exchange client. Both `binance_bot.py` (live trading) and `backtest.py`
(historical simulation) import from here, so the two can never silently
drift apart the way they did before this rewrite.

Strategy summary
-----------------
- While FLAT: draw N buy limit orders below price and N sell limit orders
  above price, evenly spaced `proportion` % apart.
- The first side to fill establishes a direction (LONG from a buy fill,
  SHORT from a sell fill). The opposite side's untouched grid orders are
  cancelled immediately (you don't want a stray grid order flipping your
  position by accident).
- The same-direction grid orders are left in place so the position can
  average in further (up to n_levels fills total) if price keeps moving
  against you.
- A single take-profit order for the *whole* averaged position is
  (re)placed every time the average entry price or position size changes.
  The TP is expressed as a percentage return on the margin used
  (`tp_pct`), so it already accounts for leverage — this matches how
  "ROI %" works on Binance Futures, not a raw price-move percentage.
- Once the TP order fills, the position is flat again and a fresh grid
  is drawn around the current price.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class GridConfig:
    symbol: str
    n_levels: int = 10                 # number of buy levels AND number of sell levels
    proportion: float = 1.5            # % spacing between adjacent grid levels
    volume: float = 0.05               # order size per grid level, in base asset units
    tp_pct: float = 3.0                # take-profit target, in % ROI on margin used
    leverage: int = 1                  # account leverage used for margin / TP math
    price_decimals: int = 1            # price rounding precision for this symbol
    stop_loss_pct: Optional[float] = None  # optional circuit breaker, in % ROI loss on
                                            # margin used. None = disabled (classic grid
                                            # behaviour: ride it out, no stop-loss). This
                                            # is the single biggest risk knob in this
                                            # strategy — see README "Risk" section.


def generate_grid_levels(center_price: float, cfg: GridConfig) -> Tuple[List[float], List[float]]:
    """
    Build N buy levels below `center_price` and N sell levels above it,
    uniformly spaced `cfg.proportion` % apart.

    Level i (1..n_levels), price offset from center = i * proportion%:
        buy[i]  = center * (1 - i * proportion / 100)
        sell[i] = center * (1 + i * proportion / 100)

    Returns (buy_levels, sell_levels), each sorted closest-to-center first.
    """
    buys, sells = [], []
    for i in range(1, cfg.n_levels + 1):
        pct = i * cfg.proportion / 100
        buys.append(round(center_price * (1 - pct), cfg.price_decimals))
        sells.append(round(center_price * (1 + pct), cfg.price_decimals))
    return buys, sells


def calculate_tp_price(entry_price: float, position_amt: float, cfg: GridConfig) -> Optional[float]:
    """
    Price at which closing `position_amt` (signed: +long / -short), opened
    at weighted-average `entry_price`, realizes `cfg.tp_pct` % return on the
    margin used for that position (an ROI target, not a raw price-move %).

        margin_used    = entry_price * |position_amt| / leverage
        target_profit  = margin_used * tp_pct / 100
        price_move     = target_profit / |position_amt|
                        = entry_price * tp_pct / (100 * leverage)
        tp_price        = entry_price + price_move   (LONG: price must rise)
                        = entry_price - price_move   (SHORT: price must fall)
    """
    if position_amt == 0:
        return None
    price_move = entry_price * cfg.tp_pct / (100 * cfg.leverage)
    if position_amt > 0:
        tp = entry_price + price_move
    else:
        tp = entry_price - price_move
    return round(tp, cfg.price_decimals)


def calculate_stop_price(entry_price: float, position_amt: float, cfg: GridConfig) -> Optional[float]:
    """
    Mirror of calculate_tp_price: the price at which closing `position_amt`
    realizes a LOSS of `cfg.stop_loss_pct` % of the margin used. Returns
    None if stop_loss_pct is not configured (disabled) or position is flat.
    """
    if position_amt == 0 or cfg.stop_loss_pct is None:
        return None
    price_move = entry_price * cfg.stop_loss_pct / (100 * cfg.leverage)
    if position_amt > 0:
        sl = entry_price - price_move   # LONG: loss when price falls
    else:
        sl = entry_price + price_move   # SHORT: loss when price rises
    return round(sl, cfg.price_decimals)


def estimate_liquidation_price(entry_price: float, position_amt: float, leverage: int) -> Optional[float]:
    """
    Rough isolated-margin liquidation price estimate, ignoring maintenance
    margin tiers, funding, and fees:

        LONG:  liq  ~= entry * (1 - 1/leverage)
        SHORT: liq  ~= entry * (1 + 1/leverage)

    This is a simplified approximation for backtesting purposes only —
    Binance's real liquidation price also depends on the maintenance
    margin bracket for the position's notional size. With leverage=1 this
    never triggers for a normal price move, matching "no leverage, no
    liquidation risk" intuition.
    """
    if position_amt == 0 or leverage <= 1:
        return None
    if position_amt > 0:
        return entry_price * (1 - 1 / leverage)
    else:
        return entry_price * (1 + 1 / leverage)


def weighted_average_entry(avg_price: float, amt: float, fill_price: float, fill_qty: float) -> float:
    """New weighted-average entry price after adding fill_qty at fill_price
    to an existing position of size `amt` at `avg_price`."""
    if amt == 0:
        return fill_price
    return (avg_price * abs(amt) + fill_price * fill_qty) / (abs(amt) + fill_qty)
