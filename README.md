# prefect-sse

A deliberately plain data pipeline, built three ways, to learn the three layers Prefect now owns:

| Layer | Tool | Role in this repo |
|---|---|---|
| Outcomes | Dagster | declares what should exist: assets, partitions, checks |
| Execution | Prefect + Prefect Cloud | runs the work: schedules, retries, workers, run history |
| Access | FastMCP (via existing MCP servers) | lets Claude Code ask questions about runs and data |

**The job:** every morning, fetch the asteroids making their closest approach to Earth that day
from NASA's Near Earth Object feed, flatten them to one row per asteroid, and append the rows to
a single DuckDB file. One request in, a handful of rows out. The API is free and takes a date in
the URL, which is what makes daily partitions and backfills meaningful later.

## Layout

```
core/neos.py            three plain functions, no framework imports
flows/neo_flow.py       Prefect wrapper: three @tasks, one @flow
prefect.yaml            deployment: schedule, work pool, repo to clone, parameters
dagster_defs/           (step 4, not yet written) Dagster assets around the same core functions
.mcp.json               (step 3, not yet written) MCP servers for Claude Code
neos.duckdb             the output, git-ignored. Table neos(date, neo_id, name, diameter_m, miss_km, velocity_kph, hazardous)
```

Everything that does real work is in `core/neos.py`. The other files are framework wiring, so
anything that differs between the Prefect and Dagster versions is that framework's opinion.

## Setup

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is pinned because Prefect and Dagster
do not support 3.14 yet; uv downloads it.

```sh
uv sync
cp .env.example .env     # then put your free key from https://api.nasa.gov in NASA_API_KEY
```

`DEMO_KEY` works without signing up but is capped at 50 requests a day, which a month-long
backfill will exceed.

## Step 1: run the core by hand

```sh
uv run --env-file .env python -m core.neos 2026-09-30
```

Prints the rows written and the three closest approaches. `neos.duckdb` appears in the repo root.
Running the same day twice appends duplicate rows on purpose; preventing that is the job of the
orchestrators in later steps.

## Step 2: Prefect

### Run the flow locally

```sh
uv run --env-file .env python -m flows.neo_flow 2026-10-01
```

Prefect starts a temporary local API server, runs `fetch -> transform -> load`, and stops it.
The `fetch` task retries three times with backoff. A `database is locked` line from the
temporary server's telemetry is harmless and disappears once Prefect Cloud is the API.

### Deploy to Prefect Cloud

Once, from your own terminal (it opens a browser):

```sh
uv run prefect cloud login
```

Then:

```sh
# 1. a queue for your laptop to poll
uv run prefect work-pool create local --type process

# 2. the NASA key, so the worker never needs .env
uv run --env-file .env python -c "from prefect.blocks.system import Secret; import os; \
  Secret(value=os.environ['NASA_API_KEY']).save('nasa-api-key')"

# 3. register the deployment described in prefect.yaml
uv run prefect deploy --all

# 4. start the worker and leave it running
uv run prefect worker start --pool local
```

What happens on each scheduled run (06:00 America/New_York, set in `prefect.yaml`):

1. Prefect Cloud creates a flow run from the schedule.
2. The worker, polling the `local` pool, picks it up.
3. The worker clones this repo at `main` into a temp directory and imports `flows/neo_flow.py:neo_flow` from there.
4. The flow runs; `load` writes to the absolute `db_path` set in `prefect.yaml`, so the rows land in this directory and not in the temp clone.
5. The worker reports state and logs back to Prefect Cloud.

Trigger a run without waiting for the schedule:

```sh
uv run prefect deployment run 'neo-flow/daily' --param day=2026-10-02
```

Prefect Cloud's free Hobby tier covers this: one workspace, five deployments, five automations,
seven days of run history.

## Step 3: access from Claude Code (not yet done)

Two existing MCP servers, no tool code:

- `prefect-mcp-server`, Prefect's official server, built on FastMCP, for "did last night's run pass?"
- `mcp-server-motherduck --read-only`, MotherDuck's official DuckDB server, for "what passed closest this week?"

## Step 4: Dagster (not yet done)

The same three steps as assets `raw_neos -> neos -> neos_table` with daily partitions, plus a
`no_duplicate_rows` asset check. Backfill a month, then materialize one day twice to watch the
check fail.

## Step 5: capstone (not yet done)

`neos_table` materializes by calling `run_deployment("neo-flow/daily")` and waiting, so Dagster
defines the outcome and Prefect executes it.

## Status

- [x] Step 1: core functions, verified against the live feed
- [x] Step 2a: Prefect flow runs locally
- [x] Step 2b: repo on GitHub, `prefect.yaml` written
- [ ] Step 2c: Prefect Cloud login, work pool, Secret block, deploy, worker
- [ ] Step 3: MCP servers in `.mcp.json`
- [ ] Step 4: Dagster assets and check
- [ ] Step 5: capstone

## Deliberately left out

- **Any theme.** Asteroids are here because the API is free, keyless for the first few runs, and date-addressable.
- **Dagster+.** No free tier, and its MCP server only talks to Dagster+. Local `dagster dev` is enough.
- **Prefect's managed work pool.** Its compute is ephemeral and cannot keep a local DuckDB file.
- **A local LLM.** Claude Code is the one asking the questions.
