from __future__ import annotations

import math
from typing import Iterable


def mae(y_true: Iterable[float], y_pred: Iterable[float]) -> float:
    errors = [abs(a - b) for a, b in zip(y_true, y_pred)]
    if not errors:
        raise ValueError("mae requires at least one value")
    return sum(errors) / len(errors)


def rmse(y_true: Iterable[float], y_pred: Iterable[float]) -> float:
    errors = [(a - b) ** 2 for a, b in zip(y_true, y_pred)]
    if not errors:
        raise ValueError("rmse requires at least one value")
    return math.sqrt(sum(errors) / len(errors))


def mape(y_true: Iterable[float], y_pred: Iterable[float]) -> float:
    ratios = []
    for actual, pred in zip(y_true, y_pred):
        if actual == 0:
            continue
        ratios.append(abs((actual - pred) / actual))
    if not ratios:
        raise ValueError("mape requires at least one non-zero actual value")
    return sum(ratios) / len(ratios)
