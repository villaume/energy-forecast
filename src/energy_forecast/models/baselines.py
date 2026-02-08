from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Sequence


@dataclass(frozen=True)
class BaselineResult:
    name: str
    y_true: list[float]
    y_pred: list[float]
    coverage: float


def _build_index(times: Sequence[datetime], values: Sequence[float]) -> dict[datetime, float]:
    return {t: v for t, v in zip(times, values)}


def naive_last_value(
    times: Sequence[datetime],
    values: Sequence[float],
    test_start: datetime,
) -> BaselineResult:
    y_true: list[float] = []
    y_pred: list[float] = []
    last_value = None
    for t, v in zip(times, values):
        if t < test_start:
            last_value = v
            continue
        if last_value is None:
            continue
        y_true.append(v)
        y_pred.append(last_value)
        last_value = v
    total = len([t for t in times if t >= test_start])
    coverage = len(y_true) / total if total else 0.0
    return BaselineResult("naive", y_true, y_pred, coverage)


def seasonal_naive(
    times: Sequence[datetime],
    values: Sequence[float],
    test_start: datetime,
    season_hours: int,
) -> BaselineResult:
    y_true: list[float] = []
    y_pred: list[float] = []
    index = _build_index(times, values)
    total = 0
    for t, v in zip(times, values):
        if t < test_start:
            continue
        total += 1
        lag_time = t - timedelta(hours=season_hours)
        if lag_time not in index:
            continue
        y_true.append(v)
        y_pred.append(index[lag_time])
    coverage = len(y_true) / total if total else 0.0
    name = f"seasonal_naive_{season_hours}h"
    return BaselineResult(name, y_true, y_pred, coverage)


def mean_baseline(
    times: Sequence[datetime],
    values: Sequence[float],
    test_start: datetime,
) -> BaselineResult:
    train_values = [v for t, v in zip(times, values) if t < test_start]
    if not train_values:
        raise ValueError("No training data available for mean baseline")
    mean_value = sum(train_values) / len(train_values)
    y_true = [v for t, v in zip(times, values) if t >= test_start]
    y_pred = [mean_value for _ in y_true]
    total = len(y_true)
    coverage = 1.0 if total else 0.0
    return BaselineResult("mean", y_true, y_pred, coverage)


def run_baselines(
    times: Sequence[datetime],
    values: Sequence[float],
    test_start: datetime,
) -> Iterable[BaselineResult]:
    yield naive_last_value(times, values, test_start)
    yield seasonal_naive(times, values, test_start, season_hours=24)
    yield seasonal_naive(times, values, test_start, season_hours=168)
    yield mean_baseline(times, values, test_start)
