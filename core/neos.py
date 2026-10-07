"""Three plain functions: fetch one day of near-Earth asteroids, flatten to rows, append to DuckDB.

No orchestration imports here on purpose. The Prefect flow and the Dagster assets are thin
wrappers around these, so anything that differs between them is the framework's opinion.

Run directly:  python -m core.neos 2026-10-05
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from typing import Any

import duckdb
import httpx

FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed"
DEFAULT_DB_PATH = "neos.duckdb"

Row = dict[str, Any]


def fetch_neos(date: str | dt.date, api_key: str | None = None) -> dict:
    """GET one day from NASA's NeoWs feed and return the raw JSON payload.

    `api_key` falls back to the NASA_API_KEY env var, then to DEMO_KEY (50 requests/day).
    """
    day = str(date)
    key = api_key or os.environ.get("NASA_API_KEY", "DEMO_KEY")
    response = httpx.get(
        FEED_URL,
        params={"start_date": day, "end_date": day, "api_key": key},
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()


def to_rows(payload: dict) -> list[Row]:
    """Flatten the feed payload to one row per asteroid, per approach day."""
    rows: list[Row] = []
    for day, neos in payload["near_earth_objects"].items():
        for neo in neos:
            approaches = neo["close_approach_data"]
            approach = next((a for a in approaches if a["close_approach_date"] == day), approaches[0])
            diameter = neo["estimated_diameter"]["meters"]
            rows.append(
                {
                    "date": dt.date.fromisoformat(day),
                    "neo_id": neo["id"],
                    "name": neo["name"],
                    "diameter_m": (diameter["estimated_diameter_min"] + diameter["estimated_diameter_max"]) / 2,
                    "miss_km": float(approach["miss_distance"]["kilometers"]),
                    "velocity_kph": float(approach["relative_velocity"]["kilometers_per_hour"]),
                    "hazardous": bool(neo["is_potentially_hazardous_asteroid"]),
                }
            )
    return rows


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS neos (
    date         DATE,
    neo_id       TEXT,
    name         TEXT,
    diameter_m   DOUBLE,
    miss_km      DOUBLE,
    velocity_kph DOUBLE,
    hazardous    BOOLEAN
)
"""

COLUMNS = ["date", "neo_id", "name", "diameter_m", "miss_km", "velocity_kph", "hazardous"]


def write_rows(rows: list[Row], db_path: str = DEFAULT_DB_PATH) -> int:
    """Append rows to the `neos` table, creating the file and table on first use. Returns rows written.

    Deliberately a plain INSERT: running the same day twice produces duplicates. That is what
    the Dagster asset check is there to catch, and what partitions and result caching prevent.
    """
    with duckdb.connect(db_path) as con:
        con.execute(CREATE_TABLE)
        if rows:
            con.executemany(
                f"INSERT INTO neos ({', '.join(COLUMNS)}) VALUES ({', '.join('?' for _ in COLUMNS)})",
                [[row[c] for c in COLUMNS] for row in rows],
            )
    return len(rows)


def main(argv: list[str]) -> None:
    day = argv[1] if len(argv) > 1 else dt.date.today().isoformat()
    db_path = argv[2] if len(argv) > 2 else DEFAULT_DB_PATH

    rows = to_rows(fetch_neos(day))
    written = write_rows(rows, db_path)
    print(f"{day}: wrote {written} rows to {db_path}")

    with duckdb.connect(db_path, read_only=True) as con:
        closest = con.execute(
            "SELECT name, round(miss_km) AS miss_km, hazardous FROM neos WHERE date = ? ORDER BY miss_km LIMIT 3",
            [day],
        ).fetchall()
    for name, miss_km, hazardous in closest:
        print(f"  {name:<22} {int(miss_km):>12,} km{'  hazardous' if hazardous else ''}")


if __name__ == "__main__":
    main(sys.argv)
