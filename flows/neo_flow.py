"""Prefect wrapper around core.neos: three tasks, one flow.

Everything that is Prefect's opinion lives in this file: retries, logging, Secret blocks, the
parameters a deployment can set, and what the run reports back to Prefect Cloud. The work
itself is in core/neos.py.

What the run reports:
- an artifact: the day's rows as a table on the run page (prefect.artifacts)
- an asset materialization: the `neos` table in MotherDuck, shown on Cloud's Assets page with
  lineage from the NeoWs feed (prefect.assets)

Run locally:  uv run --env-file .env python -m flows.neo_flow [YYYY-MM-DD]
  writes to ./neos.duckdb unless NEOS_DB_PATH is set (e.g. md:neos for MotherDuck).

The deployment in prefect.yaml sets db_path to md:neos and runs on Prefect's managed pool,
which has no disk of its own, so the data lives in MotherDuck.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.assets import Asset, AssetProperties, add_asset_metadata, materialize
from prefect.blocks.system import Secret

from core.neos import DEFAULT_DB_PATH, FEED_URL, fetch_neos, to_rows, write_rows

NASA_BLOCK = "nasa-api-key"
MOTHERDUCK_BLOCK = "motherduck-token"

# Assets: what this flow reads and what it produces. Keys are URIs; Cloud draws the lineage.
NEOWS_FEED = Asset(
    key="https://api.nasa.gov/neo/rest/v1/feed",
    properties=AssetProperties(name="NASA NeoWs feed", url=FEED_URL, description="Close approaches by day."),
)
NEOS_TABLE = Asset(
    key="md://neos/main/neos",
    properties=AssetProperties(
        name="neos table",
        url="https://app.motherduck.com",
        description="One row per asteroid per close-approach day, appended daily.",
    ),
)


def secret_or_env(block_name: str, env_var: str) -> str | None:
    """Prefer the Secret block in Prefect Cloud; fall back to the env var for local runs."""
    try:
        return Secret.load(block_name).get()
    except ValueError:  # block not created yet
        return os.environ.get(env_var)


@task(retries=3, retry_delay_seconds=[5, 15, 45], asset_deps=[NEOWS_FEED])
def fetch(day: str) -> dict:
    payload = fetch_neos(day, api_key=secret_or_env(NASA_BLOCK, "NASA_API_KEY"))
    get_run_logger().info("fetched %s: %s objects", day, payload.get("element_count"))
    return payload


@task
def transform(payload: dict) -> list[dict]:
    rows = to_rows(payload)
    get_run_logger().info("flattened to %d rows", len(rows))
    return rows


@materialize(NEOS_TABLE, by="duckdb")
def load(rows: list[dict], db_path: str) -> int:
    if db_path.startswith("md:") and "MOTHERDUCK_TOKEN" not in os.environ:
        token = secret_or_env(MOTHERDUCK_BLOCK, "MOTHERDUCK_TOKEN")
        if token:
            os.environ["MOTHERDUCK_TOKEN"] = token  # duckdb reads it when opening md: paths
    written = write_rows(rows, db_path)
    get_run_logger().info("wrote %d rows to %s", written, db_path)
    add_asset_metadata(NEOS_TABLE, {"rows_written": written, "db_path": db_path, "date": rows[0]["date"].isoformat() if rows else None})
    return written


@task
def report(day: str, rows: list[dict]) -> None:
    """Attach the day's rows to the run as a table artifact, closest first."""
    table = [
        {
            "name": r["name"],
            "miss_km": round(r["miss_km"]),
            "velocity_kph": round(r["velocity_kph"]),
            "diameter_m": round(r["diameter_m"]),
            "hazardous": r["hazardous"],
        }
        for r in sorted(rows, key=lambda r: r["miss_km"])
    ]
    create_table_artifact(
        key="neos-daily",
        table=table,
        description=f"## Close approaches on {day}\n{len(rows)} asteroids, closest first.",
    )


@flow(name="neo-flow", log_prints=True)
def neo_flow(day: str | None = None, db_path: str | None = None) -> int:
    """Fetch one day of near-Earth asteroids and append them to the neos table."""
    day = day or dt.date.today().isoformat()
    db_path = db_path or os.environ.get("NEOS_DB_PATH") or str(Path.cwd() / DEFAULT_DB_PATH)

    payload = fetch(day)
    rows = transform(payload)
    written = load(rows, db_path)
    report(day, rows)
    return written


if __name__ == "__main__":
    neo_flow(day=sys.argv[1] if len(sys.argv) > 1 else None)
