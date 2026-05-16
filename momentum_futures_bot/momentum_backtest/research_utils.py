from __future__ import annotations

import itertools
import json
import math
import os
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, as_completed, wait
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import binance
except ImportError:
    import binance_connector as binance  # type: ignore

from momentum_core.backtester import BacktestResult, RegimeBreakoutBacktester
from momentum_core.strategy import StrategyConfig, create_strategy
from momentum_backtest.run_autonomous_backtest import (
    build_pit_universe_schedule,
    cache_file_name,
    download_klines,
    parse_symbol_list,
    pick_universe,
)

_GRID_WORKER_CONTEXT: dict[str, Any] = {}


def parse_date(value: str) -> pd.Timestamp:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return pd.Timestamp(parsed)


def ensure_utc(value: pd.Timestamp | datetime | str) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.tz_localize("UTC")
        return value.tz_convert("UTC")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return pd.Timestamp(value.replace(tzinfo=UTC))
        return pd.Timestamp(value.astimezone(UTC))
    return parse_date(value)


def build_is_oos_split(
    start: pd.Timestamp,
    end: pd.Timestamp,
    is_months: int,
    oos_months: int,
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    is_start = ensure_utc(start)
    full_end = ensure_utc(end)
    is_end = min(ensure_utc(is_start + pd.DateOffset(months=is_months)), full_end)
    oos_start = is_end
    oos_end = min(ensure_utc(oos_start + pd.DateOffset(months=oos_months)), full_end)
    if oos_end < oos_start:
        oos_end = oos_start
    return is_start, is_end, oos_start, oos_end


def normalize_symbols(symbols: list[str]) -> list[str]:
    deduped: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def normalize_signal_mode(signal_mode: str | None) -> str:
    mode = str(signal_mode or StrategyConfig().signal_mode).strip().lower()
    return "tv" if mode == "tv" else "breakout"


def threshold_config_key(signal_mode: str | None) -> str:
    return "tv_threshold_values" if normalize_signal_mode(signal_mode) == "tv" else "grid_threshold_values"


def default_threshold_for_mode(signal_mode: str | None) -> float:
    return 17.0 if normalize_signal_mode(signal_mode) == "tv" else 7.5


def apply_signal_threshold(
    base_config_overrides: dict[str, Any] | None,
    signal_mode: str | None,
    threshold: float,
) -> dict[str, Any]:
    config = dict(base_config_overrides or {})
    mode = normalize_signal_mode(signal_mode or config.get("signal_mode"))
    config["signal_mode"] = mode

    if mode == "tv":
        vote_threshold = max(1, min(26, int(round(float(threshold)))))
        config["strong_buy_threshold"] = vote_threshold
        config["strong_sell_threshold"] = vote_threshold
    else:
        score_threshold = float(threshold)
        config["long_score_threshold"] = score_threshold
        config["short_score_threshold"] = score_threshold

    return config


def extract_signal_threshold(config_overrides: dict[str, Any] | None) -> float:
    config = config_overrides or {}
    mode = normalize_signal_mode(config.get("signal_mode"))
    if mode == "tv":
        value = config.get("strong_buy_threshold", config.get("strong_sell_threshold", default_threshold_for_mode(mode)))
        return float(value)
    value = config.get("long_score_threshold", config.get("short_score_threshold", default_threshold_for_mode(mode)))
    return float(value)


def build_strategy_config(overrides: dict[str, Any] | None = None) -> StrategyConfig:
    payload = asdict(StrategyConfig())
    if overrides:
        payload.update(overrides)
    return StrategyConfig(**payload)


def run_backtest(
    symbol_frames: dict[str, pd.DataFrame],
    btc_frame: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    config_overrides: dict[str, Any] | None,
    initial_equity: float,
    universe_schedule: dict[pd.Timestamp, set[str]] | None = None,
) -> BacktestResult:
    strategy = create_strategy(config=build_strategy_config(config_overrides))
    backtester = RegimeBreakoutBacktester(strategy=strategy, initial_equity=initial_equity)
    return backtester.run(
        symbol_frames=symbol_frames,
        btc_frame=btc_frame,
        start=start,
        end=end,
        universe_schedule=universe_schedule,
    )


def resolve_market_pool(
    client: binance.Futures,
    market_top_n: int,
    include_symbols: list[str],
    exclude_symbols: set[str],
    pool_symbols: list[str] | None = None,
) -> list[str]:
    if pool_symbols:
        return [symbol for symbol in normalize_symbols(pool_symbols) if symbol not in exclude_symbols]
    requested_top_n = max(market_top_n, 0)
    if requested_top_n == 0 and include_symbols:
        return [symbol for symbol in normalize_symbols(include_symbols) if symbol not in exclude_symbols]
    return pick_universe(
        client=client,
        top_n=max(requested_top_n, len(include_symbols)),
        include_symbols=include_symbols,
        exclude_symbols=exclude_symbols,
    )


def read_cached_frame(cache_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(cache_path, index_col="open_time", parse_dates=["open_time", "close_time"])
    frame.index = pd.to_datetime(frame.index, utc=True)
    return frame.sort_index()


def build_cache_cover_index(
    cache_dir: Path,
    interval: str,
) -> dict[str, list[tuple[pd.Timestamp, pd.Timestamp, Path]]]:
    index: dict[str, list[tuple[pd.Timestamp, pd.Timestamp, Path]]] = {}
    for cache_path in cache_dir.glob(f"*_{interval}_*.csv"):
        parts = cache_path.stem.rsplit("_", 3)
        if len(parts) != 4:
            continue
        symbol, cached_interval, start_token, end_token = parts
        if cached_interval != interval:
            continue
        try:
            cached_start = pd.Timestamp(datetime.strptime(start_token, "%Y%m%d"), tz="UTC")
            cached_end = pd.Timestamp(datetime.strptime(end_token, "%Y%m%d"), tz="UTC")
        except ValueError:
            continue
        index.setdefault(symbol, []).append((cached_start, cached_end, cache_path))
    return index


def load_covering_cached_frame(
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cache_candidates: list[tuple[pd.Timestamp, pd.Timestamp, Path]] | None = None,
) -> pd.DataFrame | None:
    if not cache_candidates:
        return None

    best_path: Path | None = None
    best_span: pd.Timedelta | None = None
    for cached_start, cached_end, cache_path in cache_candidates:
        if cached_start > start or cached_end < end:
            continue
        frame_span = cached_end - cached_start
        if best_path is None or best_span is None or frame_span < best_span:
            best_path = cache_path
            best_span = frame_span

    if best_path is None:
        return None

    try:
        frame = read_cached_frame(best_path)
    except Exception:
        return None
    if frame.empty:
        return None
    if frame.index.min() > start or frame.index.max() < end:
        return None
    return frame


def download_market_data(
    client: binance.Futures,
    symbols: list[str],
    interval: str,
    history_start: pd.Timestamp,
    end: pd.Timestamp,
    cache_dir: Path,
    min_history_cutoff: pd.Timestamp,
    min_bars_in_range: int,
    range_start: pd.Timestamp,
    retries: int = 3,
    progress_label: str = "download",
    log_interval_seconds: float = 60.0,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, str]]:
    regime_symbol = "BTCUSDT"
    download_symbols = [regime_symbol] + [symbol for symbol in normalize_symbols(symbols) if symbol != regime_symbol]
    requested_end = end + timedelta(days=2)
    cache_index = build_cache_cover_index(cache_dir=cache_dir, interval=interval)

    raw_frames: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}
    started_at = time.time()
    last_log_at = started_at

    for index, symbol in enumerate(download_symbols, start=1):
        exact_cache_path = cache_dir / cache_file_name(symbol=symbol, interval=interval, start=history_start, end=requested_end)
        if exact_cache_path.exists():
            try:
                frame = read_cached_frame(exact_cache_path)
            except Exception:
                frame = pd.DataFrame()
        else:
            frame = load_covering_cached_frame(
                symbol=symbol,
                start=history_start,
                end=requested_end,
                cache_candidates=cache_index.get(symbol),
            )
        if frame is not None and not frame.empty:
            raw_frames[symbol] = frame
        else:
            frame = pd.DataFrame()
            for attempt in range(retries):
                try:
                    frame = download_klines(
                        client=client,
                        symbol=symbol,
                        interval=interval,
                        start=history_start,
                        end=requested_end,
                        cache_dir=cache_dir,
                    )
                    break
                except Exception as exc:  # pragma: no cover - network/runtime dependent
                    if "1003" in str(exc) or "Too many" in str(exc):
                        time.sleep(30 * (attempt + 1))
                        continue
                    skipped[symbol] = str(exc)
                    break

            if frame.empty:
                skipped.setdefault(symbol, "empty_frame")
            else:
                raw_frames[symbol] = frame.sort_index()

        now = time.time()
        if now - last_log_at >= log_interval_seconds or index == len(download_symbols):
            print(
                f"  {progress_label}: {index}/{len(download_symbols)} processed "
                f"| loaded={len(raw_frames)} | skipped={len(skipped)} "
                f"| elapsed={(now - started_at)/60:.1f} min"
            )
            last_log_at = now

    btc_frame = raw_frames.pop(regime_symbol, pd.DataFrame())
    if btc_frame.empty:
        raise RuntimeError("BTCUSDT history is required for regime filtering.")

    symbol_frames: dict[str, pd.DataFrame] = {}
    for symbol in normalize_symbols(symbols):
        frame = raw_frames.get(symbol)
        if frame is None or frame.empty:
            skipped.setdefault(symbol, "missing_frame")
            continue
        bars_in_range = len(frame.loc[range_start:end])
        if bars_in_range <= 0:
            skipped[symbol] = "no_bars_in_range"
            continue
        symbol_frames[symbol] = frame

    return btc_frame, symbol_frames, skipped


def objective_value(summary: dict[str, Any], objective: str) -> float:
    total_return = float(summary.get("total_return_pct", 0.0) or 0.0)
    drawdown = abs(float(summary.get("max_drawdown_pct", 0.0) or 0.0))
    profit_factor = float(summary.get("profit_factor", 0.0) or 0.0)
    avg_r = float(summary.get("avg_r_multiple", 0.0) or 0.0)
    trades = int(summary.get("closed_trades", 0) or 0)

    if trades <= 0:
        return float("-inf")

    if objective == "pf":
        return profit_factor
    if objective == "return":
        return total_return
    if objective == "avg_r":
        return avg_r
    if objective == "calmar":
        return total_return / drawdown if drawdown > 0.0 else total_return
    raise ValueError(f"Unsupported objective: {objective}")


def build_grid_configs(
    threshold_values: list[float],
    stop_values: list[float],
    take1_values: list[float],
    take2_values: list[float],
    partial_values: list[float] | None,
    base_config_overrides: dict[str, Any],
    signal_mode: str | None = None,
) -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    effective_partial_values = [float(value) for value in (partial_values or [StrategyConfig().partial_exit_pct])]
    for threshold, stop_atr, take1_atr, take2_atr, partial_exit_pct in itertools.product(
        threshold_values,
        stop_values,
        take1_values,
        take2_values,
        effective_partial_values,
    ):
        if take1_atr <= stop_atr or take2_atr <= take1_atr:
            continue
        config = apply_signal_threshold(
            base_config_overrides=base_config_overrides,
            signal_mode=signal_mode or base_config_overrides.get("signal_mode"),
            threshold=float(threshold),
        )
        config.update(
            {
                "stop_atr": float(stop_atr),
                "take1_atr": float(take1_atr),
                "take2_atr": float(take2_atr),
                "partial_exit_pct": float(partial_exit_pct),
            }
        )
        configs.append(config)
    return configs


def _grid_worker(payload: tuple[Any, ...]) -> dict[str, Any]:
    if isinstance(payload, dict):
        result = run_backtest(
            symbol_frames=_GRID_WORKER_CONTEXT["symbol_frames"],
            btc_frame=_GRID_WORKER_CONTEXT["btc_frame"],
            start=_GRID_WORKER_CONTEXT["start"],
            end=_GRID_WORKER_CONTEXT["end"],
            config_overrides=payload,
            initial_equity=_GRID_WORKER_CONTEXT["initial_equity"],
            universe_schedule=_GRID_WORKER_CONTEXT["universe_schedule"],
        )
        config_overrides = payload
        summary = result.summary
        return {
            "config_overrides": config_overrides,
            "summary": summary,
            "threshold": extract_signal_threshold(config_overrides),
            "stop_atr": float(config_overrides["stop_atr"]),
            "take1_atr": float(config_overrides["take1_atr"]),
            "take2_atr": float(config_overrides["take2_atr"]),
            "partial_exit_pct": float(config_overrides.get("partial_exit_pct", StrategyConfig().partial_exit_pct)),
            "profit_factor": float(summary.get("profit_factor", 0.0) or 0.0),
            "total_return_pct": float(summary.get("total_return_pct", 0.0) or 0.0),
            "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
            "avg_r_multiple": float(summary.get("avg_r_multiple", 0.0) or 0.0),
            "closed_trades": int(summary.get("closed_trades", 0) or 0),
        }

    (
        symbol_frames,
        btc_frame,
        start,
        end,
        config_overrides,
        initial_equity,
        universe_schedule,
    ) = payload

    result = run_backtest(
        symbol_frames=symbol_frames,
        btc_frame=btc_frame,
        start=start,
        end=end,
        config_overrides=config_overrides,
        initial_equity=initial_equity,
        universe_schedule=universe_schedule,
    )
    summary = result.summary
    return {
        "config_overrides": config_overrides,
        "summary": summary,
        "threshold": extract_signal_threshold(config_overrides),
        "stop_atr": float(config_overrides["stop_atr"]),
        "take1_atr": float(config_overrides["take1_atr"]),
        "take2_atr": float(config_overrides["take2_atr"]),
        "partial_exit_pct": float(config_overrides.get("partial_exit_pct", StrategyConfig.partial_exit_pct)),
        "profit_factor": float(summary.get("profit_factor", 0.0) or 0.0),
        "total_return_pct": float(summary.get("total_return_pct", 0.0) or 0.0),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
        "avg_r_multiple": float(summary.get("avg_r_multiple", 0.0) or 0.0),
        "closed_trades": int(summary.get("closed_trades", 0) or 0),
    }


def _init_grid_worker(
    symbol_frames: dict[str, pd.DataFrame],
    btc_frame: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    initial_equity: float,
    universe_schedule: dict[pd.Timestamp, set[str]] | None,
) -> None:
    global _GRID_WORKER_CONTEXT
    _GRID_WORKER_CONTEXT = {
        "symbol_frames": symbol_frames,
        "btc_frame": btc_frame,
        "start": start,
        "end": end,
        "initial_equity": initial_equity,
        "universe_schedule": universe_schedule,
    }


def run_parameter_grid(
    symbol_frames: dict[str, pd.DataFrame],
    btc_frame: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    configs: list[dict[str, Any]],
    initial_equity: float,
    universe_schedule: dict[pd.Timestamp, set[str]] | None,
    objective: str,
    min_trades: int,
    workers: int,
    log_interval_seconds: float = 60.0,
    progress_json_path: Path | None = None,
    progress_csv_path: Path | None = None,
    existing_results: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = [
        normalize_grid_result(item=item, objective=objective, min_trades=min_trades)
        for item in (existing_results or [])
    ]
    total = len(configs) + len(results)
    completed = len(results)
    started_at = time.time()
    last_log_at = started_at

    def flush_progress() -> None:
        if progress_json_path is not None:
            payload = {
                "completed": completed,
                "total": total,
                "objective": objective,
                "min_trades": min_trades,
                "results": results,
            }
            write_json(progress_json_path, payload)
        if progress_csv_path is not None:
            progress_frame = pd.DataFrame(
                [
                    {
                        "threshold": item["threshold"],
                        "stop_atr": item["stop_atr"],
                        "take1_atr": item["take1_atr"],
                        "take2_atr": item["take2_atr"],
                        "partial_exit_pct": item["partial_exit_pct"],
                        "profit_factor": item["profit_factor"],
                        "total_return_pct": item["total_return_pct"],
                        "max_drawdown_pct": item["max_drawdown_pct"],
                        "avg_r_multiple": item["avg_r_multiple"],
                        "closed_trades": item["closed_trades"],
                        "objective_score": item["objective_score"],
                        "eligible_for_selection": item["eligible_for_selection"],
                    }
                    for item in _sort_grid_results(list(results), objective=objective, min_trades=min_trades)
                ]
            )
            progress_frame.to_csv(progress_csv_path, index=False)

    if total <= 0:
        flush_progress()
        return []

    if not configs:
        flush_progress()
        return _sort_grid_results(results=results, objective=objective, min_trades=min_trades)

    if workers > 1 and total > 1:
        try:
            with ProcessPoolExecutor(
                max_workers=workers,
                initializer=_init_grid_worker,
                initargs=(symbol_frames, btc_frame, start, end, initial_equity, universe_schedule),
            ) as pool:
                pending = {pool.submit(_grid_worker, config) for config in configs}
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=log_interval_seconds,
                        return_when=FIRST_COMPLETED,
                    )
                    now = time.time()
                    if not done:
                        elapsed = max(now - started_at, 1e-9)
                        print(
                            f"  Grid heartbeat: {completed}/{total} completed "
                            f"| active_workers~{min(len(pending), workers)} "
                            f"| elapsed {elapsed/60:.1f} min"
                        )
                        flush_progress()
                        last_log_at = now
                        continue

                    for future in done:
                        result = future.result()
                        result["objective_score"] = objective_value(result["summary"], objective)
                        result["eligible_for_selection"] = result["closed_trades"] >= min_trades
                        results.append(result)
                        completed += 1

                    if completed >= 1 and (completed == total or now - last_log_at >= log_interval_seconds or len(done) > 0):
                        elapsed = max(now - started_at, 1e-9)
                        rate = completed / elapsed
                        eta_seconds = (total - completed) / rate if rate > 0 else 0.0
                        print(
                            f"  Grid progress: {completed}/{total} "
                            f"| best {objective}={max(item['objective_score'] for item in results):.3f} "
                            f"| ETA {eta_seconds/60:.1f} min"
                        )
                        flush_progress()
                        last_log_at = now
                flush_progress()
            return _sort_grid_results(results=results, objective=objective, min_trades=min_trades)
        except Exception as exc:
            print(f"  Multiprocessing unavailable, falling back to sequential grid: {exc}")

    for config in configs:
        result = _grid_worker((symbol_frames, btc_frame, start, end, config, initial_equity, universe_schedule))
        result["objective_score"] = objective_value(result["summary"], objective)
        result["eligible_for_selection"] = result["closed_trades"] >= min_trades
        results.append(result)
        completed += 1
        now = time.time()
        if completed == 1 or completed == total or now - last_log_at >= log_interval_seconds:
            elapsed = max(time.time() - started_at, 1e-9)
            rate = completed / elapsed
            eta_seconds = (total - completed) / rate if rate > 0 else 0.0
            print(
                f"  Grid progress: {completed}/{total} "
                f"| best {objective}={max(item['objective_score'] for item in results):.3f} "
                f"| ETA {eta_seconds/60:.1f} min"
            )
            flush_progress()
            last_log_at = now

    flush_progress()
    return _sort_grid_results(results=results, objective=objective, min_trades=min_trades)


def _sort_grid_results(
    results: list[dict[str, Any]],
    objective: str,
    min_trades: int,
) -> list[dict[str, Any]]:
    results.sort(
        key=lambda item: (
            item["eligible_for_selection"],
            item["objective_score"],
            item["profit_factor"],
            item["total_return_pct"],
        ),
        reverse=True,
    )
    return results


def scan_partial_exit_values(
    symbol_frames: dict[str, pd.DataFrame],
    btc_frame: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    base_config_overrides: dict[str, Any],
    partial_values: list[float],
    initial_equity: float,
    universe_schedule: dict[pd.Timestamp, set[str]] | None,
    objective: str,
    min_trades: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for partial_exit_pct in partial_values:
        config_overrides = dict(base_config_overrides)
        config_overrides["partial_exit_pct"] = float(partial_exit_pct)
        result = run_backtest(
            symbol_frames=symbol_frames,
            btc_frame=btc_frame,
            start=start,
            end=end,
            config_overrides=config_overrides,
            initial_equity=initial_equity,
            universe_schedule=universe_schedule,
        )
        summary = result.summary
        results.append(
            {
                "config_overrides": config_overrides,
                "summary": summary,
                "partial_exit_pct": float(partial_exit_pct),
                "profit_factor": float(summary.get("profit_factor", 0.0) or 0.0),
                "total_return_pct": float(summary.get("total_return_pct", 0.0) or 0.0),
                "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
                "avg_r_multiple": float(summary.get("avg_r_multiple", 0.0) or 0.0),
                "closed_trades": int(summary.get("closed_trades", 0) or 0),
                "objective_score": objective_value(summary, objective),
                "eligible_for_selection": int(summary.get("closed_trades", 0) or 0) >= min_trades,
            }
        )

    results.sort(
        key=lambda item: (
            item["eligible_for_selection"],
            item["objective_score"],
            item["profit_factor"],
        ),
        reverse=True,
    )
    return results


def aggregate_symbol_performance(trades_frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "symbol",
        "trades",
        "pnl",
        "profit_factor",
        "win_rate_pct",
        "avg_r",
        "median_r",
        "avg_signal_score",
        "tp1_hit_rate_pct",
    ]
    if trades_frame.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for symbol, group in trades_frame.groupby("symbol"):
        wins = group["gross_net_pnl"] > 0
        gross_profit = float(group.loc[wins, "gross_net_pnl"].sum())
        gross_loss = float(group.loc[~wins, "gross_net_pnl"].sum())
        profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else math.inf
        rows.append(
            {
                "symbol": symbol,
                "trades": int(len(group)),
                "pnl": float(group["gross_net_pnl"].sum()),
                "profit_factor": float(profit_factor),
                "win_rate_pct": float(wins.mean() * 100.0),
                "avg_r": float(group["r_multiple"].mean()),
                "median_r": float(group["r_multiple"].median()),
                "avg_signal_score": float(group["signal_score"].mean()),
                "tp1_hit_rate_pct": float(group["tp1_hit"].mean() * 100.0),
            }
        )

    report = pd.DataFrame(rows)
    report.sort_values(["pnl", "avg_r", "profit_factor"], ascending=[False, False, False], inplace=True)
    return report.reset_index(drop=True)


def select_fixed_universe(
    symbol_report: pd.DataFrame,
    final_top_n: int,
    min_symbol_trades: int,
    rank_by: str,
) -> pd.DataFrame:
    report = symbol_report.copy()
    if report.empty:
        report["eligible"] = pd.Series(dtype=bool)
        report["selected"] = pd.Series(dtype=bool)
        report["selection_rank"] = pd.Series(dtype=int)
        return report

    sort_map = {
        "pnl": ["pnl", "avg_r", "profit_factor", "trades"],
        "avg_r": ["avg_r", "profit_factor", "pnl", "trades"],
        "pf": ["profit_factor", "avg_r", "pnl", "trades"],
    }
    if rank_by not in sort_map:
        raise ValueError(f"Unsupported symbol rank metric: {rank_by}")

    report["eligible"] = (
        (report["trades"] >= int(min_symbol_trades))
        & (report["pnl"] > 0)
        & (report["avg_r"] > 0)
        & (report["profit_factor"] > 1.0)
    )

    sort_columns = ["eligible"] + sort_map[rank_by]
    ascending = [False] * len(sort_columns)
    report.sort_values(sort_columns, ascending=ascending, inplace=True)
    report.reset_index(drop=True, inplace=True)
    report["selection_rank"] = np.arange(1, len(report) + 1, dtype=int)
    report["selected"] = False

    selected_indexes = report.index[: max(final_top_n, 0)]
    report.loc[selected_indexes, "selected"] = True
    return report


def serialize_universe_schedule(schedule: dict[pd.Timestamp, set[str]]) -> dict[str, list[str]]:
    return {
        ensure_utc(timestamp).isoformat(): sorted(symbols)
        for timestamp, symbols in sorted(schedule.items(), key=lambda item: item[0])
    }


def deserialize_universe_schedule(raw_schedule: dict[str, list[str]]) -> dict[pd.Timestamp, set[str]]:
    schedule: dict[pd.Timestamp, set[str]] = {}
    for timestamp, symbols in raw_schedule.items():
        schedule[parse_date(timestamp)] = set(normalize_symbols(symbols))
    return schedule


def collect_required_symbols(selected_universe: dict[str, Any], market_pool_symbols: list[str]) -> list[str]:
    mode = selected_universe.get("mode")
    if mode == "fixed":
        return normalize_symbols(selected_universe.get("symbols", []))
    if mode == "pit":
        schedule = deserialize_universe_schedule(selected_universe.get("pit_schedule", {}))
        symbols: list[str] = []
        for members in schedule.values():
            for symbol in sorted(members):
                if symbol not in symbols:
                    symbols.append(symbol)
        return symbols
    return normalize_symbols(market_pool_symbols)


def sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize_for_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_for_json(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return ensure_utc(value).isoformat()
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(sanitize_for_json(payload), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_config_key(config_overrides: dict[str, Any] | None) -> str:
    config = config_overrides or {}
    payload = {
        "signal_mode": normalize_signal_mode(config.get("signal_mode")),
        "threshold": extract_signal_threshold(config),
        "stop_atr": float(config.get("stop_atr", 0.0) or 0.0),
        "take1_atr": float(config.get("take1_atr", 0.0) or 0.0),
        "take2_atr": float(config.get("take2_atr", 0.0) or 0.0),
        "partial_exit_pct": float(config.get("partial_exit_pct", StrategyConfig().partial_exit_pct)),
    }
    return json.dumps(sanitize_for_json(payload), sort_keys=True, ensure_ascii=False)


def normalize_grid_result(
    item: dict[str, Any],
    objective: str,
    min_trades: int,
) -> dict[str, Any]:
    summary = dict(item.get("summary", {}))
    config_overrides = dict(item.get("config_overrides", {}))
    result = {
        "config_overrides": config_overrides,
        "summary": summary,
        "threshold": float(item.get("threshold", extract_signal_threshold(config_overrides))),
        "stop_atr": float(item.get("stop_atr", config_overrides.get("stop_atr", 0.0)) or 0.0),
        "take1_atr": float(item.get("take1_atr", config_overrides.get("take1_atr", 0.0)) or 0.0),
        "take2_atr": float(item.get("take2_atr", config_overrides.get("take2_atr", 0.0)) or 0.0),
        "partial_exit_pct": float(item.get("partial_exit_pct", config_overrides.get("partial_exit_pct", StrategyConfig().partial_exit_pct))),
        "profit_factor": float(item.get("profit_factor", summary.get("profit_factor", 0.0)) or 0.0),
        "total_return_pct": float(item.get("total_return_pct", summary.get("total_return_pct", 0.0)) or 0.0),
        "max_drawdown_pct": float(item.get("max_drawdown_pct", summary.get("max_drawdown_pct", 0.0)) or 0.0),
        "avg_r_multiple": float(item.get("avg_r_multiple", summary.get("avg_r_multiple", 0.0)) or 0.0),
        "closed_trades": int(item.get("closed_trades", summary.get("closed_trades", 0)) or 0),
    }
    result["objective_score"] = float(item.get("objective_score", objective_value(summary, objective)))
    result["eligible_for_selection"] = bool(item.get("eligible_for_selection", result["closed_trades"] >= min_trades))
    return result
