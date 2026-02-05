from __future__ import annotations

from datetime import datetime, timedelta, timezone
import base64
import random
import time
from typing import Iterator
from zoneinfo import ZoneInfo

import dlt
import requests

TIBBER_API_URL = "https://api.tibber.com/v1-beta/gql"


def _post_graphql(token: str, query: str, variables: dict) -> dict:
    max_retries = 6
    backoff_base = 2.0
    for attempt in range(max_retries + 1):
        response = requests.post(
            TIBBER_API_URL,
            json={"query": query, "variables": variables},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            if response.status_code == 429 and attempt < max_retries:
                delay = backoff_base**attempt + random.uniform(0.0, 0.5)
                time.sleep(delay)
                continue
            body = response.text.strip()
            raise RuntimeError(f"Tibber HTTP error {response.status_code}: {body}") from exc
        payload = response.json()
        break
    if "errors" in payload:
        messages = ", ".join(
            error.get("message", "Unknown error") for error in payload["errors"]
        )
        raise RuntimeError(f"Tibber API error: {messages}")
    return payload


def fetch_consumption(
    token: str,
    home_id: str,
    last_hours: int,
    resolution: str = "HOURLY",
) -> Iterator[dict]:
    query = """
    query Consumption($homeId: ID!, $resolution: EnergyResolution!, $last: Int!) {
      viewer {
        home(id: $homeId) {
          consumption(resolution: $resolution, last: $last) {
            nodes {
              from
              to
              consumption
              cost
              unitPrice
              currency
            }
          }
        }
      }
    }
    """
    variables = {"homeId": home_id, "resolution": resolution, "last": last_hours}
    payload = _post_graphql(token=token, query=query, variables=variables)
    nodes = (
        payload.get("data", {})
        .get("viewer", {})
        .get("home", {})
        .get("consumption", {})
        .get("nodes", [])
    )
    for node in nodes:
        yield {
            "home_id": home_id,
            "from_time": node.get("from"),
            "to_time": node.get("to"),
            "consumption": node.get("consumption"),
            "cost": node.get("cost"),
            "unit_price": node.get("unitPrice"),
            "currency": node.get("currency"),
        }


def _parse_start(value: str, tz: ZoneInfo) -> datetime:
    if "T" in value:
        dt = datetime.fromisoformat(value)
    else:
        dt = datetime.fromisoformat(f"{value}T00:00:00")
    return dt.astimezone(tz) if dt.tzinfo else dt.replace(tzinfo=tz)


def _encode_after_cursor(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc).replace(tzinfo=None)
    cursor = utc_value.strftime("%Y-%m-%dT%H:%M:%S")
    return base64.b64encode(cursor.encode("utf-8")).decode("utf-8")


def fetch_consumption_range(
    token: str,
    home_id: str,
    start: datetime,
    end: datetime,
    resolution: str = "HOURLY",
) -> Iterator[dict]:
    query = """
    query ConsumptionRange($homeId: ID!, $resolution: EnergyResolution!, $after: String!, $first: Int!) {
      viewer {
        home(id: $homeId) {
          consumption(resolution: $resolution, after: $after, first: $first) {
            nodes {
              from
              to
              consumption
              cost
              unitPrice
              currency
            }
          }
        }
      }
    }
    """
    first = int((end - start).total_seconds() // 3600)
    if first <= 0:
        return
    variables = {
        "homeId": home_id,
        "resolution": resolution,
        "after": _encode_after_cursor(start),
        "first": first,
    }
    payload = _post_graphql(token=token, query=query, variables=variables)
    nodes = (
        payload.get("data", {})
        .get("viewer", {})
        .get("home", {})
        .get("consumption", {})
        .get("nodes", [])
    )
    for node in nodes:
        yield {
            "home_id": home_id,
            "from_time": node.get("from"),
            "to_time": node.get("to"),
            "consumption": node.get("consumption"),
            "cost": node.get("cost"),
            "unit_price": node.get("unitPrice"),
            "currency": node.get("currency"),
        }


def iter_consumption_chunks(
    token: str,
    home_id: str,
    start: datetime,
    end: datetime,
    chunk_hours: int,
    resolution: str = "HOURLY",
) -> Iterator[dict]:
    if chunk_hours <= 0:
        raise ValueError("chunk_hours must be positive")
    if start >= end:
        raise ValueError("start must be before end")
    current = start
    while current < end:
        chunk_end = min(current + timedelta(hours=chunk_hours), end)
        chunk_rows = fetch_consumption_range(
            token=token,
            home_id=home_id,
            start=current,
            end=chunk_end,
            resolution=resolution,
        )
        if chunk_rows:
            yield from chunk_rows
        current = chunk_end


@dlt.source
def tibber_source(
    token: str,
    home_id: str,
    last_hours: int = 720,
    start: str | None = None,
    end: str | None = None,
    chunk_hours: int = 168,
):
    tz = ZoneInfo("Europe/Stockholm")
    if start:
        parsed_start = _parse_start(start, tz)
        parsed_end = _parse_start(end, tz) if end else datetime.now(tz)
        resource = dlt.resource(
            iter(
                iter_consumption_chunks(
                    token=token,
                    home_id=home_id,
                    start=parsed_start,
                    end=parsed_end,
                    chunk_hours=chunk_hours,
                )
            ),
            name="consumption",
            primary_key=("home_id", "from_time"),
        )
    else:
        resource = dlt.resource(
            iter(
                fetch_consumption(token=token, home_id=home_id, last_hours=last_hours)
            ),
            name="consumption",
            primary_key=("home_id", "from_time"),
        )
    return [resource]
