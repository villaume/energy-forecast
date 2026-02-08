from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os

import psycopg2
from dotenv import load_dotenv

from energy_forecast.evaluation.metrics import mae, mape, rmse
from energy_forecast.models.baselines import run_baselines


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


def main() -> None:
    load_dotenv()
    window_days = int(os.getenv("BASELINE_WINDOW_DAYS", "90"))
    test_days = int(os.getenv("BASELINE_TEST_DAYS", "7"))

    times, values = fetch_series(window_days)
    max_time = max(times)
    test_start = max_time - timedelta(days=test_days)

    print(f"Window: last {window_days} days")
    print(f"Test start (UTC): {test_start.astimezone(timezone.utc).isoformat()}")

    for result in run_baselines(times, values, test_start):
        y_true = result.y_true
        y_pred = result.y_pred
        if not y_true:
            print(f"{result.name}: no predictions")
            continue
        metrics = {
            "mae": mae(y_true, y_pred),
            "rmse": rmse(y_true, y_pred),
            "mape": mape(y_true, y_pred),
        }
        print(
            f"{result.name} | coverage={result.coverage:.2%} "
            f"mae={metrics['mae']:.4f} rmse={metrics['rmse']:.4f} mape={metrics['mape']:.2%}"
        )


if __name__ == "__main__":
    main()
