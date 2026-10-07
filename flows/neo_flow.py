"""Prefect wrapper around core.neos: three tasks, one flow.

Everything that is Prefect's opinion lives in this file: retries, logging, the Secret block,
and the parameters a deployment can set. The work itself is in core/neos.py.

Run locally:  uv run --env-file .env python -m flows.neo_flow [YYYY-MM-DD]
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

from core.neos import DEFAULT_DB_PATH, fetch_neos, to_rows, write_rows

SECRET_BLOCK = "nasa-api-key"


def nasa_api_key() -> str | None:
    """Prefer the Secret block in Prefect Cloud; fall back to the env var for local runs."""
    try:
        return Secret.load(SECRET_BLOCK).get()
    except ValueError:  # block not created yet
        return os.environ.get("NASA_API_KEY")


@task(retries=3, retry_delay_seconds=[5, 15, 45])
def fetch(day: str) -> dict:
    payload = fetch_neos(day, api_key=nasa_api_key())
    get_run_logger().info("fetched %s: %s objects", day, payload.get("element_count"))
    return payload


@task
def transform(payload: dict) -> list[dict]:
    rows = to_rows(payload)
    get_run_logger().info("flattened to %d rows", len(rows))
    return rows


@task
def load(rows: list[dict], db_path: str) -> int:
    written = write_rows(rows, db_path)
    get_run_logger().info("wrote %d rows to %s", written, db_path)
    return written


@flow(name="neo-flow", log_prints=True)
def neo_flow(day: str | None = None, db_path: str | None = None) -> int:
    """Fetch one day of near-Earth asteroids and append them to DuckDB.

    `db_path` must be absolute when run from a deployment: the worker executes in a fresh
    clone of the repo, so a relative path would land in a temp directory and vanish.
    """
    day = day or dt.date.today().isoformat()
    db_path = db_path or os.environ.get("NEOS_DB_PATH") or str(Path.cwd() / DEFAULT_DB_PATH)

    payload = fetch(day)
    rows = transform(payload)
    return load(rows, db_path)


if __name__ == "__main__":
    neo_flow(day=sys.argv[1] if len(sys.argv) > 1 else None)
