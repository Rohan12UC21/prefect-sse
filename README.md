# prefect-sse

Daily near-Earth asteroid pipeline, built three ways to compare the three layers Prefect now owns:
**Prefect** executes it, **Dagster** declares it, **FastMCP** gives an AI agent access to it.

Every morning the flow fetches the asteroids making their closest approach to Earth that day from
[NASA's NeoWs feed](https://api.nasa.gov), flattens them to one row per asteroid, and appends the
rows to a `neos` table in [MotherDuck](https://motherduck.com). It runs on Prefect Cloud's managed
pool, is deployed from this repo by GitHub Actions, and pings Discord if it fails.

```
  push          GitHub Actions           Prefect Cloud (free tier)
 ──────▶ repo ──────────────────▶ deployment neo-flow/daily, cron 06:00
            ▲ clone                          │ starts a container
            │                                ▼
         managed run: fetch ──▶ transform ──▶ load ──▶ MotherDuck  md:neos
                        │                                    ▲
                   NASA NeoWs API              Dagster assets (local) and
                                               Claude Code via MCP read/write here
```

The step-by-step walkthrough, concepts, and what was learned live in the
[Asteroid Pipeline Field Guide](https://claude.ai/code/artifact/7bf37196-0aa2-4499-b63c-01cd42d9c2c4).

## Quick start

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is pinned (Prefect and Dagster do not
support 3.14 yet); uv downloads it.

```sh
uv sync
cp .env.example .env            # fill in NASA_API_KEY and MOTHERDUCK_TOKEN
uv run --env-file .env python -m core.neos 2026-09-30            # core only, local DuckDB file
uv run --env-file .env python -m flows.neo_flow 2026-09-30       # Prefect flow, local
uv run --env-file .env dagster dev                               # Dagster UI on :3000
```

## Layout

| Path | What it is |
| --- | --- |
| `core/neos.py` | `fetch_neos`, `to_rows`, `write_rows`. Plain Python, no framework imports. |
| `flows/neo_flow.py` | Prefect flow: three tasks with retries, Secret blocks, a table artifact, an asset materialization. |
| `prefect.yaml` | Deployment: managed work pool, cron schedule, repo to clone, `db_path: md:neos`. |
| `.github/workflows/prefect-deploy.yml` | On push to `main`: sync the MotherDuck token into a Secret block, `prefect deploy --all`. |
| `dagster_defs/definitions.py` | Dagster: assets `raw_neos -> neos -> neos_table` with daily partitions, check `no_duplicate_rows`. |
| `.mcp.json`, `scripts/motherduck-mcp.sh` | MCP servers for Claude Code: Prefect (official, FastMCP) and MotherDuck (official). |

Table schema: `neos(date, neo_id, name, diameter_m, miss_km, velocity_kph, hazardous)`.

## Configuration

| Where | Name | Purpose |
| --- | --- | --- |
| `.env` | `NASA_API_KEY` | free key from api.nasa.gov; `DEMO_KEY` works but is capped at 50 requests/day |
| `.env` | `MOTHERDUCK_TOKEN` | MotherDuck access token, for local runs and the MCP server |
| `.env` | `NEOS_DB_PATH` | where Dagster writes: `md:neos` or a local file |
| `.env` | `DAGSTER_HOME` | keeps Dagster run history across restarts |
| Prefect Cloud blocks | `nasa-api-key`, `motherduck-token` (Secret), `discord-failures` (Webhook) | what the managed run and the failure automation read |
| GitHub Actions secrets | `PREFECT_API_KEY`, `PREFECT_API_URL`, `MOTHERDUCK_API_KEY` | what the deploy workflow needs |

## Operating it

```sh
uv run prefect cloud login                                            # once
uv run prefect deployment run 'neo-flow/daily' --param day=2026-10-02 --watch
uv run prefect deployment run 'neo-flow/daily' --param day=not-a-date --watch   # forces a failure, tests the Discord ping
uv run --env-file .env dagster asset materialize --select '*' --partition 2026-09-15 -m dagster_defs.definitions
```

In Prefect Cloud: the run page shows logs and the `neos-daily` table artifact; the Assets page shows
the `neos table` asset with lineage from the NASA feed; Automations shows the Discord failure ping.

## Status

All five planned steps are done except the optional capstone, where a Dagster asset materializes
by triggering the Prefect deployment. See the field guide for what was verified at each step.
