from __future__ import annotations

import itertools
import json
import os
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .data import ensure_ohlcv_frame
from .engine import BacktestResult, ExecutionParams, StrategyParams, run_backtest


# --- Globals для worker-процессов (заполняются через initializer) ---
_worker_data: pd.DataFrame | None = None
_worker_exec_data: pd.DataFrame | None = None
_worker_execution: ExecutionParams | None = None
_worker_symbol: str = ""
_worker_objective: str = ""
_worker_constraints: dict[str, float] | None = None


def _worker_init(
    data_bytes: bytes,
    exec_data_bytes: bytes | None,
    execution: ExecutionParams,
    symbol: str,
    objective: str,
    constraints: dict[str, float] | None,
) -> None:
    global _worker_data, _worker_exec_data, _worker_execution
    global _worker_symbol, _worker_objective, _worker_constraints
    _worker_data = pd.read_parquet(pd.io.common.BytesIO(data_bytes))
    _worker_data.attrs["_sloping_ohlcv_normalized"] = True
    if exec_data_bytes is not None:
        _worker_exec_data = pd.read_parquet(pd.io.common.BytesIO(exec_data_bytes))
        _worker_exec_data.attrs["_sloping_ohlcv_normalized"] = True
    else:
        _worker_exec_data = None
    _worker_execution = execution
    _worker_symbol = symbol
    _worker_objective = objective
    _worker_constraints = constraints


def _worker_evaluate(params_dict: dict[str, Any]) -> dict[str, Any] | None:
    try:
        params = StrategyParams(**params_dict)
        result = run_backtest(
            _worker_data,
            params,
            _worker_execution,
            symbol=_worker_symbol,
            execution_data=_worker_exec_data,
        )
        score = _score_summary(result.summary, _worker_objective, _worker_constraints)
        return {
            **params_dict,
            **{f"train_{k}": v for k, v in result.summary.items()},
            "train_score": score,
        }
    except (ValueError, Exception):
        return None


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = pd.io.common.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


def default_max_workers() -> int:
    """~half of logical cores — leaves headroom for OS."""
    cores = os.cpu_count() or 4
    return max(2, cores // 2 - 2)  # 32 threads -> 14 workers


@dataclass
class OptimizationResult:
    train_results: pd.DataFrame
    validation_results: pd.DataFrame | None
    best_params: dict[str, Any]
    best_train_summary: dict[str, Any]
    best_validation_summary: dict[str, Any] | None
    validation_start: pd.Timestamp | None


def load_search_space(path: str | Path) -> dict[str, dict[str, Any]]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _strategy_field_names() -> set[str]:
    return set(StrategyParams.__dataclass_fields__.keys())


def _discrete_values(spec: dict[str, Any]) -> list[Any] | None:
    spec_type = spec["type"]
    if spec_type == "bool":
        return [False, True]
    if spec_type == "choice":
        return list(spec["values"])
    if spec_type == "int":
        step = int(spec.get("step", 1))
        return list(range(int(spec["min"]), int(spec["max"]) + 1, step))
    if spec_type == "float" and "step" in spec:
        start = float(spec["min"])
        stop = float(spec["max"])
        step = float(spec["step"])
        count = int(round((stop - start) / step))
        return [round(start + step * idx, 10) for idx in range(count + 1)]
    return None


def _sample_value(spec: dict[str, Any], rng: random.Random) -> Any:
    spec_type = spec["type"]
    if spec_type == "bool":
        return rng.choice([False, True])
    if spec_type == "choice":
        return rng.choice(list(spec["values"]))
    if spec_type == "int":
        return rng.choice(_discrete_values(spec))
    if spec_type == "float":
        if "step" in spec:
            return rng.choice(_discrete_values(spec))
        return rng.uniform(float(spec["min"]), float(spec["max"]))
    raise ValueError(f"Unsupported search space type: {spec_type}")


def _search_space_size(search_space: dict[str, dict[str, Any]]) -> int | None:
    total = 1
    for spec in search_space.values():
        values = _discrete_values(spec)
        if values is None:
            return None
        total *= len(values)
    return total


def _canonicalize_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(candidate)
    if normalized.get("use_trend_filter") is False and "trend_sma" in normalized:
        normalized["trend_sma"] = StrategyParams().trend_sma
    return normalized


def _is_valid_candidate(params: dict[str, Any]) -> bool:
    take1 = params.get("take1")
    take2 = params.get("take2")
    if take1 is not None and take2 is not None and float(take2) > 0 and float(take2) < float(take1):
        return False
    return True


def _generate_candidates(
    search_space: dict[str, dict[str, Any]],
    trials: int,
    seed: int,
) -> list[dict[str, Any]]:
    size = _search_space_size(search_space)
    if size is not None and size <= trials:
        keys = list(search_space.keys())
        grids = [_discrete_values(search_space[key]) for key in keys]
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[tuple[str, Any], ...]] = set()
        for combo in itertools.product(*grids):
            candidate = dict(zip(keys, combo))
            if not _is_valid_candidate(candidate):
                continue
            fingerprint = tuple(sorted(_canonicalize_candidate(candidate).items()))
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            candidates.append(candidate)
        return candidates

    rng = random.Random(seed)
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, Any], ...]] = set()
    max_attempts = max(trials * 50, 1000)
    attempts = 0

    while len(candidates) < trials and attempts < max_attempts:
        attempts += 1
        candidate = {name: _sample_value(spec, rng) for name, spec in search_space.items()}
        if not _is_valid_candidate(candidate):
            continue
        fingerprint = tuple(sorted(_canonicalize_candidate(candidate).items()))
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        candidates.append(candidate)

    return candidates


def _score_summary(
    summary: dict[str, Any],
    objective: str,
    constraints: dict[str, float] | None,
) -> float:
    constraints = constraints or {}

    if summary["trades"] < constraints.get("min_trades", 0):
        return float("-inf")
    if summary["profit_factor"] < constraints.get("min_profit_factor", 0.0):
        return float("-inf")
    if summary["win_rate_pct"] < constraints.get("min_win_rate_pct", 0.0):
        return float("-inf")
    if summary["profit_month_ratio_pct"] < constraints.get("min_profit_month_ratio_pct", 0.0):
        return float("-inf")

    if objective == "return_pct":
        return float(summary["return_pct"])
    if objective == "net_profit":
        return float(summary["net_profit"])
    if objective == "profit_factor":
        return float(summary["profit_factor"])
    if objective == "win_rate_pct":
        return float(summary["win_rate_pct"])
    if objective == "sharpe":
        return float(summary["sharpe"])
    if objective == "return_over_max_drawdown":
        return float(summary["return_over_max_drawdown"])
    if objective == "composite":
        return float(
            summary["return_pct"]
            - summary["max_drawdown_pct"]
            + 0.5 * summary["profit_month_ratio_pct"]
            + 0.25 * summary["win_rate_pct"]
        )
    raise ValueError(f"Unsupported objective: {objective}")


def _split_validation(
    data: pd.DataFrame,
    validation_fraction: float,
) -> tuple[pd.DataFrame, pd.Timestamp]:
    if not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be in (0, 1)")
    split_index = int(len(data) * (1.0 - validation_fraction))
    split_index = max(min(split_index, len(data) - 1), 1)
    validation_start = pd.Timestamp(data.index[split_index])
    train_data = data.iloc[:split_index].copy()
    return train_data, validation_start


def _evaluate_candidate(
    data: pd.DataFrame,
    params: StrategyParams,
    execution: ExecutionParams,
    execution_data: pd.DataFrame | None,
    symbol: str,
    objective: str,
    constraints: dict[str, float] | None,
    *,
    trade_start: pd.Timestamp | None = None,
) -> tuple[BacktestResult, float]:
    result = run_backtest(
        data,
        params,
        execution,
        symbol=symbol,
        trade_start=trade_start,
        execution_data=execution_data,
    )
    score = _score_summary(result.summary, objective, constraints)
    return result, score


def optimize_parameters(
    data: pd.DataFrame,
    search_space: dict[str, dict[str, Any]],
    *,
    base_params: dict[str, Any] | None = None,
    execution: ExecutionParams | None = None,
    execution_data: pd.DataFrame | None = None,
    symbol: str = "UNKNOWN",
    trials: int = 100,
    seed: int = 42,
    objective: str = "return_over_max_drawdown",
    constraints: dict[str, float] | None = None,
    validation_fraction: float = 0.0,
    validation_top_k: int = 10,
    max_workers: int | None = None,
) -> OptimizationResult:
    invalid_fields = set(search_space) - _strategy_field_names()
    if invalid_fields:
        raise ValueError(f"Search space contains unknown strategy fields: {sorted(invalid_fields)}")

    execution = execution or ExecutionParams()
    base_params = base_params or {}
    normalized_data = ensure_ohlcv_frame(data)
    normalized_execution_data = ensure_ohlcv_frame(execution_data) if execution_data is not None else None

    defaults = asdict(StrategyParams())
    merged_base = {**defaults, **base_params}
    train_data = normalized_data
    train_execution_data = normalized_execution_data
    validation_execution_data = normalized_execution_data
    validation_start: pd.Timestamp | None = None

    if validation_fraction:
        train_data, validation_start = _split_validation(normalized_data, validation_fraction)
        if normalized_execution_data is not None:
            train_execution_data = normalized_execution_data.loc[normalized_execution_data.index < validation_start]
            validation_execution_data = normalized_execution_data.loc[normalized_execution_data.index >= validation_start]

    # --- Подготовка кандидатов ---
    candidates = _generate_candidates(search_space, trials, seed)
    all_params = []
    for candidate in candidates:
        full_params = {**merged_base, **candidate}
        if _is_valid_candidate(full_params):
            all_params.append(full_params)

    workers = max_workers if max_workers else default_max_workers()
    print(f"Optimization: {len(all_params)} candidates, {workers} workers")

    # --- Параллельный прогон (или последовательный при workers=1) ---
    train_rows: list[dict[str, Any]] = []
    if workers > 1:
        data_bytes = _df_to_parquet_bytes(train_data)
        exec_bytes = _df_to_parquet_bytes(train_execution_data) if train_execution_data is not None else None
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_worker_init,
            initargs=(data_bytes, exec_bytes, execution, symbol, objective, constraints),
        ) as pool:
            futures = {pool.submit(_worker_evaluate, p): p for p in all_params}
            done_count = 0
            for future in as_completed(futures):
                done_count += 1
                if done_count % 10 == 0 or done_count == len(futures):
                    print(f"  [{done_count}/{len(futures)}]")
                row = future.result()
                if row is not None:
                    train_rows.append(row)
    else:
        for i, full_params in enumerate(all_params):
            try:
                params = StrategyParams(**full_params)
                result, score = _evaluate_candidate(
                    train_data,
                    params,
                    execution,
                    train_execution_data,
                    symbol,
                    objective,
                    constraints,
                )
            except ValueError:
                continue
            train_rows.append(
                {
                    **full_params,
                    **{f"train_{key}": value for key, value in result.summary.items()},
                    "train_score": score,
                }
            )
            if (i + 1) % 10 == 0:
                print(f"  [{i + 1}/{len(all_params)}]")

    train_df = pd.DataFrame(train_rows)
    if not train_df.empty:
        train_df = train_df.sort_values(
            by=["train_score", "train_return_pct", "train_profit_factor"],
            ascending=False,
            na_position="last",
        ).reset_index(drop=True)

    best_train_params = {
        key: train_df.iloc[0][key]
        for key in _strategy_field_names()
        if key in train_df.columns
    } if not train_df.empty else merged_base
    best_train_summary = {
        key.removeprefix("train_"): train_df.iloc[0][key]
        for key in train_df.columns
        if key.startswith("train_")
    } if not train_df.empty else {}

    if validation_start is None or train_df.empty:
        return OptimizationResult(
            train_results=train_df,
            validation_results=None,
            best_params=best_train_params,
            best_train_summary=best_train_summary,
            best_validation_summary=None,
            validation_start=None,
        )

    validation_rows: list[dict[str, Any]] = []
    best_train_row = train_df.iloc[0]
    params_dict = {key: best_train_row[key] for key in _strategy_field_names()}
    try:
        params = StrategyParams(**params_dict)
        result, score = _evaluate_candidate(
            normalized_data,
            params,
            execution,
            validation_execution_data,
            symbol,
            objective,
            constraints,
            trade_start=validation_start,
        )
        validation_rows.append(
            {
                **params_dict,
                **{key: best_train_row[key] for key in best_train_row.index if key.startswith("train_")},
                **{f"validation_{key}": value for key, value in result.summary.items()},
                "validation_score": score,
            }
        )
    except ValueError:
        pass

    validation_df = pd.DataFrame(validation_rows)
    if validation_df.empty:
        return OptimizationResult(
            train_results=train_df,
            validation_results=validation_df,
            best_params=best_train_params,
            best_train_summary=best_train_summary,
            best_validation_summary=None,
            validation_start=validation_start,
        )

    best_validation_summary = {
        key.removeprefix("validation_"): validation_df.iloc[0][key]
        for key in validation_df.columns
        if key.startswith("validation_")
    }

    return OptimizationResult(
        train_results=train_df,
        validation_results=validation_df,
        best_params=best_train_params,
        best_train_summary=best_train_summary,
        best_validation_summary=best_validation_summary,
        validation_start=validation_start,
    )
