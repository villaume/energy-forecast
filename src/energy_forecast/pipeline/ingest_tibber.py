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
        "--resume",
        action="store_true",
        default=os.getenv("TIBBER_RESUME", "").lower() in {"1", "true", "yes"},
        help="Resume from last loaded timestamp in destination.",
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


def main() -> None:
    load_dotenv()
    args = parse_args()

    token = _env("TIBBER_TOKEN")
    home_id = _env("TIBBER_HOME_ID")
    supabase_url = _env("SUPABASE_DATABASE_URL")

    os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = supabase_url

    start_override = args.start
    if args.resume:
        last_loaded = _fetch_last_loaded(supabase_url, args.dataset, home_id)
        if last_loaded:
            start_override = last_loaded

    pipeline = dlt.pipeline(
        pipeline_name="tibber",
        destination="postgres",
        dataset_name=args.dataset,
    )
    source = tibber_source(
        token=token,
        home_id=home_id,
        last_hours=args.last_hours,
        start=start_override,
        end=args.end,
        chunk_hours=args.chunk_hours,
    )
    load_info = pipeline.run(source, write_disposition="merge")
    print(load_info)


if __name__ == "__main__":
    main()
