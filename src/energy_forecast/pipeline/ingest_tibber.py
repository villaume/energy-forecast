from __future__ import annotations

import argparse
import os

import dlt
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

from energy_forecast.data.tibber_source import tibber_source


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Tibber consumption data to Supabase via dlt."
    )
    parser.add_argument(
        "--last-hours",
        type=int,
        default=int(os.getenv("TIBBER_LAST_HOURS", "720")),
        help="How many recent hours to pull from Tibber (default: 720).",
    )
    parser.add_argument(
        "--latest-hours",
        type=int,
        default=int(os.getenv("TIBBER_LATEST_HOURS", "0")),
        help="Fetch a rolling window of the latest hours (e.g., 24).",
    )
    parser.add_argument(
        "--offset-hours",
        type=int,
        default=int(os.getenv("TIBBER_OFFSET_HOURS", "0")),
        help="Offset the end time backwards by N hours to avoid partial data.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=os.getenv("TIBBER_RESUME", "").lower() in {"1", "true", "yes"},
        help="Resume from last loaded timestamp in destination.",
    )
    parser.add_argument(
        "--self-heal",
        action="store_true",
        default=os.getenv("TIBBER_SELF_HEAL", "").lower() in {"1", "true", "yes"},
        help="After load, check for gaps in the window and retry once if any are found.",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=os.getenv("TIBBER_START"),
        help="Start datetime (YYYY-MM-DD or ISO with timezone).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=os.getenv("TIBBER_END"),
        help="End datetime (YYYY-MM-DD or ISO with timezone). Defaults to now when --start is set.",
    )
    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=int(os.getenv("TIBBER_CHUNK_HOURS", "168")),
        help="Chunk size in hours for range pulls (default: 168).",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=os.getenv("DLT_DATASET", "raw"),
        help="Destination dataset/schema (default: raw).",
    )
    return parser.parse_args()


def _fetch_last_loaded(
    conn_str: str,
    dataset: str,
    home_id: str,
) -> str | None:
    query = sql.SQL(
        "select max(from_time) from {}.consumption where home_id = %s"
    ).format(sql.Identifier(dataset))
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (home_id,))
            row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return row[0].isoformat()


def _count_gaps_in_window(
    conn_str: str,
    dataset: str,
    home_id: str,
    start: str,
    end: str,
) -> int:
    query = f"""
        with ordered as (
            select from_time,
                   lag(from_time) over (order by from_time) as prev_time
            from {dataset}.consumption
            where home_id = %s
              and from_time >= %s
              and from_time < %s
        )
        select count(*)
        from ordered
        where prev_time is not null
          and from_time - prev_time > interval '1 hour'
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (home_id, start, end))
            return int(cur.fetchone()[0])


def _write_status(
    conn_str: str,
    pipeline_name: str,
    status: str,
    message: str | None,
    rows_loaded: int | None,
) -> None:
    create_sql = """
        create table if not exists public.pipeline_status (
            pipeline_name text primary key,
            last_run_at timestamptz not null,
            status text not null,
            message text,
            rows_loaded integer
        )
    """
    upsert_sql = """
        insert into public.pipeline_status
            (pipeline_name, last_run_at, status, message, rows_loaded)
        values
            (%s, now(), %s, %s, %s)
        on conflict (pipeline_name) do update
            set last_run_at = excluded.last_run_at,
                status = excluded.status,
                message = excluded.message,
                rows_loaded = excluded.rows_loaded
    """
    msg = message[:1000] if message else None
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                cur.execute(upsert_sql, (pipeline_name, status, msg, rows_loaded))
    except Exception as exc:  # best-effort status
        print(f"Status write failed: {exc}")


def main() -> None:
    load_dotenv()
    args = parse_args()

    token = _env("TIBBER_TOKEN")
    home_id = _env("TIBBER_HOME_ID")
    supabase_url = _env("SUPABASE_DATABASE_URL")

    os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = supabase_url

    start_override = args.start
    end_override = args.end
    if args.latest_hours and args.latest_hours > 0:
        from zoneinfo import ZoneInfo
        from datetime import datetime, timedelta

        tz = ZoneInfo("Europe/Stockholm")
        end_dt = datetime.now(tz) - timedelta(hours=max(args.offset_hours, 0))
        start_dt = end_dt - timedelta(hours=args.latest_hours)
        start_override = start_dt.isoformat()
        end_override = end_dt.isoformat()
    if args.resume:
        last_loaded = _fetch_last_loaded(supabase_url, args.dataset, home_id)
        if last_loaded:
            start_override = last_loaded

    pipeline = dlt.pipeline(
        pipeline_name="tibber",
        destination="postgres",
        dataset_name=args.dataset,
    )
    try:
        source = tibber_source(
            token=token,
            home_id=home_id,
            last_hours=args.last_hours,
            start=start_override,
            end=end_override,
            chunk_hours=args.chunk_hours,
        )
        load_info = pipeline.run(source, write_disposition="merge")
        print(load_info)

        if args.self_heal and start_override and end_override:
            gaps = _count_gaps_in_window(
                supabase_url,
                args.dataset,
                home_id,
                start_override,
                end_override,
            )
            if gaps > 0:
                retry_source = tibber_source(
                    token=token,
                    home_id=home_id,
                    last_hours=args.last_hours,
                    start=start_override,
                    end=end_override,
                    chunk_hours=args.chunk_hours,
                )
                retry_info = pipeline.run(retry_source, write_disposition="merge")
                print(retry_info)

        _write_status(
            supabase_url,
            pipeline.pipeline_name,
            "success",
            str(load_info),
            None,
        )
    except Exception as exc:
        _write_status(
            supabase_url,
            pipeline.pipeline_name,
            "failed",
            str(exc),
            None,
        )
        raise


if __name__ == "__main__":
    main()
