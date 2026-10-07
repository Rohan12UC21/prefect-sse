"""Dagster wrapper around core.neos: three assets, one check, daily partitions.

Everything that is Dagster's opinion lives here: what should exist (assets), how it is sliced
(one partition per day), what must be true of it (the check). The work is in core/neos.py.

Run the UI:        uv run --env-file .env dagster dev        (http://localhost:3000)
Materialize once:  uv run --env-file .env dagster asset materialize \
                       --select '*' --partition 2026-10-06 -m dagster_defs.definitions
"""

import os

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    ConfigurableResource,
    DailyPartitionsDefinition,
    Definitions,
    EnvVar,
    MaterializeResult,
    asset,
    asset_check,
)

from core.neos import connect, fetch_neos, to_rows, write_rows

daily = DailyPartitionsDefinition(start_date="2026-09-01")


class NeosDatabase(ConfigurableResource):
    """Where the neos table lives. md:neos for MotherDuck, or a local file path."""

    db_path: str


@asset(partitions_def=daily, group_name="neos", description="Raw NeoWs feed payload for one day.")
def raw_neos(context: AssetExecutionContext) -> dict:
    day = context.partition_key
    payload = fetch_neos(day, api_key=os.environ.get("NASA_API_KEY"))
    context.log.info("fetched %s: %s objects", day, payload.get("element_count"))
    return payload


@asset(partitions_def=daily, group_name="neos", description="One row per asteroid for the day.")
def neos(context: AssetExecutionContext, raw_neos: dict) -> list[dict]:
    rows = to_rows(raw_neos)
    context.log.info("flattened to %d rows", len(rows))
    return rows


@asset(partitions_def=daily, group_name="neos", description="The day's rows appended to the neos table.")
def neos_table(context: AssetExecutionContext, neos: list[dict], db: NeosDatabase) -> MaterializeResult:
    written = write_rows(neos, db.db_path)
    context.log.info("wrote %d rows to %s", written, db.db_path)
    return MaterializeResult(metadata={"rows_written": written, "db_path": db.db_path})


@asset_check(asset=neos_table, description="No (date, neo_id) pair appears more than once.")
def no_duplicate_rows(db: NeosDatabase) -> AssetCheckResult:
    with connect(db.db_path, read_only=True) as con:
        dupes = con.execute(
            "SELECT date, neo_id, count(*) AS n FROM neos GROUP BY date, neo_id HAVING n > 1 ORDER BY date"
        ).fetchall()
    return AssetCheckResult(
        passed=not dupes,
        metadata={"duplicate_pairs": len(dupes), "sample": str(dupes[:5])},
    )


defs = Definitions(
    assets=[raw_neos, neos, neos_table],
    asset_checks=[no_duplicate_rows],
    resources={"db": NeosDatabase(db_path=EnvVar("NEOS_DB_PATH"))},
)
