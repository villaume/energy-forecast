from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Sequence

from energy_forecast.models.baselines import BaselineResult, mean_baseline, naive_last_value, seasonal_naive


@dataclass(frozen=True)
class BacktestWindow:
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


def generate_windows(
    times: Sequence[datetime],
    train_days: int,
    test_days: int,
    step_days: int,
) -> Iterable[BacktestWindow]:
    if not times:
        return
    start = min(times)
    end = max(times)
    train_delta = timedelta(days=train_days)
    test_delta = timedelta(days=test_days)
    step_delta = timedelta(days=step_days)

    train_start = start
    train_end = train_start + train_delta
    while train_end + test_delta <= end:
        test_start = train_end
        test_end = test_start + test_delta
        yield BacktestWindow(train_start, train_end, test_start, test_end)
        train_end += step_delta


def _filter_series(
    times: Sequence[datetime],
    values: Sequence[float],
    start: datetime,
    end: datetime,
) -> tuple[list[datetime], list[float]]:
    filtered_times: list[datetime] = []
    filtered_values: list[float] = []
    for t, v in zip(times, values):
        if start <= t < end:
            filtered_times.append(t)
            filtered_values.append(v)
    return filtered_times, filtered_values


def run_baseline_backtest(
    times: Sequence[datetime],
    values: Sequence[float],
    train_days: int,
    test_days: int,
    step_days: int,
) -> Iterable[tuple[BacktestWindow, list[BaselineResult]]]:
    for window in generate_windows(times, train_days, test_days, step_days):
        window_times, window_values = _filter_series(times, values, window.train_start, window.test_end)
        test_start = window.test_start
        results = [
            naive_last_value(window_times, window_values, test_start),
            seasonal_naive(window_times, window_values, test_start, season_hours=24),
            seasonal_naive(window_times, window_values, test_start, season_hours=168),
            mean_baseline(window_times, window_values, test_start),
        ]
        yield window, results
