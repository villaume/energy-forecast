from __future__ import annotations

import os

import psycopg2
import psycopg2.extensions
import pytest


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


@pytest.fixture(scope="session")
def db_conn() -> psycopg2.extensions.connection:
    conn_str = _env("SUPABASE_DATABASE_URL")
    conn = psycopg2.connect(conn_str)
    try:
        yield conn
    finally:
        conn.close()


def _dataset() -> str:
    return os.getenv("DLT_DATASET", "raw")


def _home_id() -> str:
    return _env("TIBBER_HOME_ID")


def test_consumption_coverage_2025_q4(db_conn: psycopg2.extensions.connection) -> None:
    if not os.getenv("SUPABASE_DATABASE_URL"):
        pytest.skip("SUPABASE_DATABASE_URL not set")
    dataset = _dataset()
    home_id = _home_id()
    query = f"""
        select
            min(from_time) <= (timestamp '2024-09-01 00:00:00' at time zone 'Europe/Stockholm') as min_ok,
            max(from_time) >= (timestamp '2026-01-31 23:00:00' at time zone 'Europe/Stockholm') as max_ok
        from {dataset}.consumption
        where home_id = %s
    """
    with db_conn.cursor() as cur:
        cur.execute(query, (home_id,))
        row = cur.fetchone()
    assert row and row[0] is not None, "No data found in consumption table"
    min_ok, max_ok = row
    assert min_ok, "min_time is after 2024-09-01 00:00 Europe/Stockholm"
    assert max_ok, "max_time is before 2026-01-31 23:00 Europe/Stockholm"


def test_no_missing_hours_2025_q4(db_conn: psycopg2.extensions.connection) -> None:
    if not os.getenv("SUPABASE_DATABASE_URL"):
        pytest.skip("SUPABASE_DATABASE_URL not set")
    dataset = _dataset()
    home_id = _home_id()
    query = f"""
        with ordered as (
            select from_time,
                   lag(from_time) over (order by from_time) as prev_time
            from {dataset}.consumption
            where home_id = %s
              and from_time >= (timestamp '2024-09-01 00:00:00' at time zone 'Europe/Stockholm')
              and from_time < (timestamp '2026-01-01 00:00:00' at time zone 'Europe/Stockholm')
        )
        select count(*)
        from ordered
        where prev_time is not null
          and from_time - prev_time > interval '1 hour'
    """
    with db_conn.cursor() as cur:
        cur.execute(query, (home_id,))
        gaps = cur.fetchone()[0]
    if gaps:
        details_query = f"""
            with ordered as (
                select from_time,
                       lag(from_time) over (order by from_time) as prev_time
                from {dataset}.consumption
                where home_id = %s
                  and from_time >= (timestamp '2024-09-01 00:00:00' at time zone 'Europe/Stockholm')
                  and from_time < (timestamp '2026-01-01 00:00:00' at time zone 'Europe/Stockholm')
            )
            select prev_time, from_time, from_time - prev_time as gap
            from ordered
            where prev_time is not null
              and from_time - prev_time > interval '1 hour'
            order by gap desc
            limit 5
        """
        with db_conn.cursor() as cur:
            cur.execute(details_query, (home_id,))
            rows = cur.fetchall()
        formatted = "; ".join(f"{prev} -> {curr} ({gap})" for prev, curr, gap in rows)
        pytest.fail(
            f"Found {gaps} gaps larger than 1 hour between 2024-09-01 and 2025-12-31. Examples: {formatted}"
        )


def test_no_duplicate_entries(db_conn: psycopg2.extensions.connection) -> None:
    if not os.getenv("SUPABASE_DATABASE_URL"):
        pytest.skip("SUPABASE_DATABASE_URL not set")
    dataset = _dataset()
    home_id = _home_id()
    query = f"""
        select count(*)
        from (
            select home_id, from_time, count(*)
            from {dataset}.consumption
            where home_id = %s
            group by home_id, from_time
            having count(*) > 1
        ) duplicates
    """
    with db_conn.cursor() as cur:
        cur.execute(query, (home_id,))
        dupes = cur.fetchone()[0]
    assert dupes == 0, f"Found {dupes} duplicate (home_id, from_time) rows"
