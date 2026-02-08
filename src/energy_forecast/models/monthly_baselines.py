from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence


@dataclass(frozen=True)
class MonthlyForecast:
    model: str
    forecast_month: date
    value_kwh: float
    horizon_months: int


def _add_months(value: date, months: int) -> date:
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    return date(year, month, 1)


def _monthly_index(months: Sequence[date], values: Sequence[float]) -> dict[date, float]:
    return {m: v for m, v in zip(months, values)}


def seasonal_last_year(
    months: Sequence[date],
    values: Sequence[float],
    horizon: int,
) -> list[MonthlyForecast]:
    index = _monthly_index(months, values)
    last_month = max(months)
    forecasts: list[MonthlyForecast] = []
    for step in range(1, horizon + 1):
        target = _add_months(last_month, step)
        last_year = _add_months(target, -12)
        if last_year not in index:
            continue
        forecasts.append(
            MonthlyForecast(
                model="seasonal_last_year",
                forecast_month=target,
                value_kwh=index[last_year],
                horizon_months=step,
            )
        )
    return forecasts


def rolling_mean(
    months: Sequence[date],
    values: Sequence[float],
    horizon: int,
    window_months: int,
) -> list[MonthlyForecast]:
    if window_months <= 0:
        raise ValueError("window_months must be positive")
    if len(values) < window_months:
        return []
    last_month = max(months)
    mean_value = sum(values[-window_months:]) / window_months
    return [
        MonthlyForecast(
            model=f"rolling_mean_{window_months}m",
            forecast_month=_add_months(last_month, step),
            value_kwh=mean_value,
            horizon_months=step,
        )
        for step in range(1, horizon + 1)
    ]


def run_monthly_baselines(
    months: Sequence[date],
    values: Sequence[float],
    horizon: int,
    rolling_window_months: int,
) -> list[MonthlyForecast]:
    forecasts: list[MonthlyForecast] = []
    forecasts.extend(seasonal_last_year(months, values, horizon))
    forecasts.extend(rolling_mean(months, values, horizon, rolling_window_months))
    return forecasts
