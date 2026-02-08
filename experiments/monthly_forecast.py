from __future__ import annotations

from datetime import date
import os

import psycopg2
from dotenv import load_dotenv

from energy_forecast.models.monthly_baselines import MonthlyForecast, run_monthly_baselines


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def fetch_monthly_kwh() -> tuple[list[date], list[float]]:
    conn_str = _env("SUPABASE_DATABASE_URL")
    home_id = _env("TIBBER_HOME_ID")
    dataset = os.getenv("DLT_DATASET", "raw")
    query = f"""
        select
            date_trunc('month', from_time at time zone 'Europe/Stockholm')::date as month_start,
            sum(consumption) as kwh
        from {dataset}.consumption
        where home_id = %s
          and consumption is not null
        group by 1
        order by 1
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (home_id,))
            rows = cur.fetchall()
    months = [row[0] for row in rows]
    values = [float(row[1]) for row in rows]
    if not months:
        raise RuntimeError("No monthly data returned from Supabase")
    return months, values


def ensure_table(conn: psycopg2.extensions.connection) -> None:
    sql = """
        create table if not exists public.monthly_forecasts (
            id bigserial primary key,
            model text not null,
            forecast_month date not null,
            created_at timestamptz not null default now(),
            value_kwh double precision not null,
            horizon_months integer not null
        )
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def insert_forecasts(
    conn: psycopg2.extensions.connection,
    forecasts: list[MonthlyForecast],
) -> None:
    if not forecasts:
        return
    sql = """
        insert into public.monthly_forecasts
            (model, forecast_month, value_kwh, horizon_months)
        values
            (%s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        for forecast in forecasts:
            cur.execute(
                sql,
                (
                    forecast.model,
                    forecast.forecast_month,
                    forecast.value_kwh,
                    forecast.horizon_months,
                ),
            )


def main() -> None:
    load_dotenv()
    horizon = int(os.getenv("MONTHLY_FORECAST_HORIZON", "3"))
    rolling_window = int(os.getenv("MONTHLY_ROLLING_WINDOW", "6"))

    months, values = fetch_monthly_kwh()
    forecasts = run_monthly_baselines(months, values, horizon, rolling_window)

    if not forecasts:
        print("No forecasts produced (insufficient history)")
        return

    for forecast in forecasts:
        print(
            f"{forecast.model} | month={forecast.forecast_month} "
            f"h={forecast.horizon_months} value={forecast.value_kwh:.2f} kWh"
        )

    conn_str = _env("SUPABASE_DATABASE_URL")
    with psycopg2.connect(conn_str) as conn:
        ensure_table(conn)
        insert_forecasts(conn, forecasts)
        conn.commit()


if __name__ == "__main__":
    main()
