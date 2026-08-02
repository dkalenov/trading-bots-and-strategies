# Williams Fractal Breakout Strategy

A Williams Fractal–based swing-structure breakout strategy for crypto (or
any OHLCV market): signal generation, a risk-managed backtest engine
(stop-loss, take-profit, fixed-fractional position sizing), and
charting — runnable end-to-end from the command line.

> **Disclaimer:** Educational project. Not financial advice. Crypto and
> leveraged trading carry a high risk of loss, including total loss of
> capital. Backtest results do not guarantee future performance —
> **no backtest is, or can be, 100% accurate** (see "Honest limits"
> below for exactly why, and what this project does to stay
> conservative about it). Nothing here should be run against a real,
> funded account without your own independent testing, paper-trading,
> and risk management on top of it.

## How it works

1. **Fractal detection** (`src/fractals.py`) — a classic 5-bar Williams
   Fractal: a bar is a *fractal high* if its high is strictly greater
   than the 2 bars before and after it (and symmetrically for lows).
2. **Swing structure** — fractal pivots are consolidated into a
   zig-zag of alternating swing highs/lows (keeping the most extreme
   point of any run of same-type pivots, not just the first one found).
3. **Signal**:
   - **LONG** — a higher low forms (swing low > previous swing low),
     and price then breaks **above** the swing high sitting between
     them (bullish break of structure).
   - **SHORT** — a lower high forms (swing high < previous swing high),
     and price then breaks **below** the swing low sitting between
     them (bearish break of structure).
   - Each signal also carries a `stop_level`: the swing point that
     defined the setup — the natural point at which the pattern is
     itself invalidated.
4. **Risk-managed backtest** (`src/backtest.py`):
   - A signal is only acted on at the **next bar's open** — never the
     bar that produced it (no look-ahead).
   - Every position has a **stop-loss** and a **take-profit**, checked
     against every bar's high/low from entry onward.
   - **Position size is risk-based** (fixed-fractional): you set how
     much equity you're willing to lose if the stop is hit
     (`risk_per_trade`), and the position size is solved backwards
     from the distance to the stop — a tight stop gets a smaller
     position, a wide stop gets a larger one, capped by `max_leverage`.
   - Fees are charged on entry and exit, proportional to the actual
     notional traded (not the whole account).

## Honest limits — read this before trusting any number

No backtest built from OHLC candles can be 100% accurate, and that's a
data limitation, not something more code can fully close:

- **Intrabar path is unknown.** A candle only tells you the open,
  high, low and close — not the order in which price moved. If a
  bar's range touches *both* your stop and your target, there is no
  way to know from the candle alone which was hit first.
- **No real order-book / slippage data.** Fills here assume you get
  exactly the stop or target price (with an exception for gaps — see
  below), which is optimistic compared to a real, especially illiquid
  or fast-moving, market.
- **No funding fees, liquidation mechanics, or latency.** Relevant for
  perpetual futures specifically; not modeled here.

What this project does instead is make every assumption explicit and
lean toward the conservative side rather than hide the ambiguity:

| Situation | Assumption made |
|---|---|
| Stop and target both inside the same bar's range | **Stop is assumed hit first** (worse case for you) |
| Bar's open already gapped past the stop | Filled at that **open price**, not the stop price (gap risk isn't hidden) |
| Bar's open/range passed the take-profit | Filled **at the take-profit price** — modeled as a resting limit order, never assumed better |
| Signal generated at bar close | Executed at the **next bar's open**, never the same bar |

If you want to stress-test how much these assumptions matter for your
use case, the trade log (`output/trades_*.csv`) records `exit_reason`
for every trade, so you can see exactly how many exits were
stop-outs vs. take-profits vs. signal flips, and re-run with
different `--stop-mode` / `--reward-risk` / date ranges to see how
sensitive the result is.

## Position sizing & stop placement

```
stop distance %   = |entry_price - stop_price| / entry_price
position_fraction = min(risk_per_trade / stop_distance_%, max_leverage)
take_profit        = entry_price ± reward_risk_ratio × stop_distance
```

- `--risk-per-trade` (default `0.01` = risk 1% of current equity per trade)
- `--stop-mode` (default `structure`):
  - `structure` — stop at the swing point that defined the setup
  - `atr` — stop at `entry ± atr_multiplier × ATR(atr_period)`
  - `percent` — stop at a fixed `--stop-pct`
- `--reward-risk` (default `2.0`) — take-profit distance as a multiple of the stop distance
- `--max-leverage` (default `1.0`) — hard cap on notional exposure as a fraction of equity; `1.0` means the position can never exceed 100% of equity (no leverage), regardless of how tight the stop is

Note: in the trade log, `return_pct` is the **raw price-move return**
(entry to exit), not scaled by position size — useful for judging the
setup itself (e.g. R-multiples). The actual equity impact of each
trade is `position_fraction × return_pct`, and is what the equity
curve and `pnl` column reflect.

## Project layout

| File | Purpose |
|---|---|
| `src/data_loader.py` | Downloads & caches Binance historical klines (no API key needed) |
| `src/fractals.py` | Fractal detection + swing-structure signal generation |
| `src/indicators.py` | ATR (used for the `atr` stop mode) |
| `src/backtest.py` | Backtest engine: execution, stop-loss/take-profit, position sizing, metrics |
| `src/plotting.py` | Price chart with entries/exits, equity curve chart |
| `run_backtest.py` | CLI: runs the whole pipeline end-to-end for one symbol |
| `run_multi_backtest.py` | CLI: same pipeline across many symbols, with an aggregated comparison report |
| `examples/quickstart.ipynb` | Minimal notebook walkthrough |

## Quick start

```bash
git clone <your-repo-url>
cd williams_fractal_strategy
pip install -r requirements.txt

python run_backtest.py --symbol BTCUSDT --interval 1h --start 2024-11 --end 2025-02 \
    --risk-per-trade 0.01 --stop-mode structure --reward-risk 2.0 --max-leverage 1.0
```

This will:
- download & cache `BTCUSDT` 1h klines from Binance's public data
  archive for Nov 2024 – Feb 2025 (first run only; cached after that),
- generate signals with stop-loss/take-profit levels,
- run the risk-managed backtest and print a report (return, drawdown,
  Sharpe, win rate, profit factor, stop-outs vs. take-profits vs.
  signal-flip closes),
- save a trade log CSV and two charts under `output/` — the price
  chart marks entries (▲/▼) and exits (✕ = stopped out, ★ =
  take-profit, ◆ = closed on an opposite signal).

Run `python run_backtest.py --help` for all options.

## Testing across many symbols (recommended before trusting any result)

A strategy that only looks good on one hand-picked symbol is much more
likely to be overfit or lucky than one that holds up broadly. Run it
across many pairs at once and look at the *distribution* of outcomes,
not just your favorite one:

```bash
python run_multi_backtest.py \
    --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,LINKUSDT \
    --interval 1h --start 2024-11 --end 2025-02 \
    --risk-per-trade 0.01 --stop-mode structure --reward-risk 2.0 --max-leverage 1.0
```

This runs the full pipeline independently per symbol (own trade log,
own charts, own starting capital — **not** a combined portfolio) and
writes, under `output_batch/`:

- `<SYMBOL>/trades.csv`, `<SYMBOL>/price_signals.png`, `<SYMBOL>/equity_curve.png` per symbol
- `summary.csv` — every metric for every symbol, one row each
- `SUMMARY.md` — the same thing as a readable markdown table, plus
  median return/drawdown across symbols and a note on how many symbols
  ended up profitable

Symbols with no signals in the date range, or that fail to download,
are skipped and listed separately rather than breaking the run.

**Already have bulk historical data on disk?** Skip downloading
entirely with `--input-csv`, pointing at one combined CSV with columns
`date, open, high, low, close, volume, symbol` (case-insensitive, any
order):

```bash
python run_multi_backtest.py \
    --input-csv my_bulk_klines.csv --interval 4h \
    --risk-per-trade 0.01 --stop-mode structure --reward-risk 2.0
```

With `--input-csv` and no `--symbols`, every symbol found in the file
is used — this is how `results/` in this repo was produced (328
symbols from one bulk export). Use `--symbols` to restrict to a list,
or `--max-symbols N` to just cap how many are processed. With many
symbols, only the top/bottom `--plot-top-n` (default 5) by return get
charts — set it to `0` to chart everyone, or `--no-plot` to skip charts
entirely and just get the tables (much faster for a first pass over
hundreds of symbols).

Run `python run_multi_backtest.py --help` for all options.

## Example: real results across 328 symbols

`results/2024-05_2025-10_4h_328symbols/` in this repo is exactly this
pipeline run on a real bulk 4h export (328 Binance USDT-margined pairs,
2024-05 to 2025-10, ~1.4 years) with `--risk-per-trade 0.01
--stop-mode structure --reward-risk 2.0 --max-leverage 1.0`. Open
`SUMMARY.md` there for the full picture — headline numbers:

- 180 / 328 symbols (54.9%) finished with positive return over the period
- median return **+1.39%**, mean **+3.45%** — i.e. modestly positive but
  with a long tail: a handful of symbols drove much of the average
- median max drawdown **-12.3%**, ranging from -4% to -30% depending on symbol
- this is one 1.4-year sample on one strategy variant — not a
  guarantee, and not evidence the parameters here are optimal for any
  specific symbol going forward. Re-run on your own date ranges and
  parameter choices before drawing conclusions.

## Using your own data

Any CSV with `date, open, high, low, close, volume` columns works:

```bash
python run_backtest.py --csv my_data.csv --symbol MYPAIR --interval 1h
```

## What this is *not*

- Not a live-trading bot — it doesn't place real orders or connect to
  an exchange account. It generates signals and evaluates them
  historically.
- Not a guarantee of anything, and not "100% accurate" — see "Honest
  limits" above. This is a starting point for your own research and
  paper-trading, not a finished, validated trading system.
- Not tax, legal, or financial advice.

## License

MIT — see `LICENSE`.
