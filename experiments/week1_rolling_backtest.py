from __future__ import annotations

from datetime import datetime, timezone
import os

import psycopg2
from dotenv import load_dotenv

from energy_forecast.evaluation.cross_validation import run_baseline_backtest
from energy_forecast.evaluation.metrics import mae, mape, rmse


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def fetch_series(days: int) -> tuple[list[datetime], list[float]]:
    conn_str = _env("SUPABASE_DATABASE_URL")
    home_id = _env("TIBBER_HOME_ID")
    dataset = os.getenv("DLT_DATASET", "raw")
    query = f"""
        select from_time, consumption
        from {dataset}.consumption
        where home_id = %s
          and from_time >= (now() at time zone 'UTC') - interval %s
        order by from_time
    """
    interval = f"{days} days"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (home_id, interval))
            rows = cur.fetchall()
    times = [row[0] for row in rows if row[1] is not None]
    values = [float(row[1]) for row in rows if row[1] is not None]
    if not times:
        raise RuntimeError("No data returned from Supabase")
    return times, values


def summarize(results: list[float]) -> float:
    return sum(results) / len(results) if results else float("nan")


def main() -> None:
    load_dotenv()
    window_days = int(os.getenv("BASELINE_WINDOW_DAYS", "120"))
    train_days = int(os.getenv("BACKTEST_TRAIN_DAYS", "60"))
    test_days = int(os.getenv("BACKTEST_TEST_DAYS", "7"))
    step_days = int(os.getenv("BACKTEST_STEP_DAYS", "7"))

    times, values = fetch_series(window_days)

    metrics_by_model: dict[str, list[float]] = {"mae": [], "rmse": [], "mape": []}
    per_model: dict[str, dict[str, list[float]]] = {}

    for window, results in run_baseline_backtest(times, values, train_days, test_days, step_days):
        window_label = (
            f"{window.test_start.astimezone(timezone.utc).date()}"
            f" -> {window.test_end.astimezone(timezone.utc).date()}"
        )
        print(f"Window {window_label}")
        for result in results:
            if not result.y_true:
                print(f"  {result.name}: no predictions")
                continue
            model_metrics = {
                "mae": mae(result.y_true, result.y_pred),
                "rmse": rmse(result.y_true, result.y_pred),
                "mape": mape(result.y_true, result.y_pred),
            }
            per_model.setdefault(result.name, {"mae": [], "rmse": [], "mape": []})
            for key, value in model_metrics.items():
                per_model[result.name][key].append(value)
            print(
                f"  {result.name} | coverage={result.coverage:.2%} "
                f"mae={model_metrics['mae']:.4f} rmse={model_metrics['rmse']:.4f} "
                f"mape={model_metrics['mape']:.2%}"
            )

    print("\nSummary (average across windows)")
    for model_name, metrics in per_model.items():
        print(
            f"{model_name} | "
            f"mae={summarize(metrics['mae']):.4f} "
            f"rmse={summarize(metrics['rmse']):.4f} "
            f"mape={summarize(metrics['mape']):.2%}"
        )


if __name__ == "__main__":
    main()
