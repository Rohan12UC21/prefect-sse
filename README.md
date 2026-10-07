# prefect-sse

A deliberately plain data pipeline, built three ways, to learn the three layers Prefect now owns:

| Layer | Tool | Role in this repo |
|---|---|---|
| Outcomes | Dagster | declares what should exist: assets, partitions, checks |
| Execution | Prefect + Prefect Cloud | runs the work: schedules, retries, managed compute, run history |
| Access | FastMCP (via existing MCP servers) | lets Claude Code ask questions about runs and data |

**The job:** every morning, fetch the asteroids making their closest approach to Earth that day
from NASA's Near Earth Object feed, flatten them to one row per asteroid, and append the rows to
a `neos` table. One request in, a handful of rows out. The API is free and takes a date in the
URL, which is what makes daily partitions and backfills meaningful later.

**Where things run.** Prefect Cloud's free Hobby tier only executes flows on its own managed
serverless pool, which has no persistent disk. So the scheduled flow writes to MotherDuck, a
hosted DuckDB with a free tier, and nothing in the cloud ever touches your laptop. Local runs
can still write to a local `neos.duckdb` file for poking around.

```
            ┌────────────────┐  push   ┌────────────────┐  prefect deploy  ┌────────────────┐
  you  ───▶ │  GitHub repo   │ ──────▶ │ GitHub Actions │ ───────────────▶ │  Prefect Cloud │
            └────────────────┘         └────────────────┘                  │  schedule 06:00│
                    ▲ clone                                                └───────┬────────┘
                    │                                                              │ starts a container
            ┌───────┴────────┐  GET /feed   ┌─────────────┐    INSERT   ┌──────────▼─────────┐
            │  managed run   │ ───────────▶ │  NASA NeoWs │             │     MotherDuck     │
            │ fetch→transform│ ◀─────────── │             │   ◀──────── │   db neos, table   │
            │     →load      │              └─────────────┘             │        neos        │
            └────────────────┘                                          └────────────────────┘
```

## Layout

```
core/neos.py                      three plain functions, no framework imports
flows/neo_flow.py                 Prefect wrapper: three @tasks, one @flow
prefect.yaml                      deployment: managed pool, schedule, repo to clone, db_path=md:neos
.github/workflows/prefect-deploy.yml   runs `prefect deploy --all` on every push to main
dagster_defs/                     (step 4, not yet written) Dagster assets around the same core functions
.mcp.json                         MCP servers for Claude Code: prefect, motherduck
scripts/motherduck-mcp.sh         loads .env, then starts MotherDuck's MCP server read-only on md:neos
```

The table, wherever it lives: `neos(date, neo_id, name, diameter_m, miss_km, velocity_kph, hazardous)`.

Everything that does real work is in `core/neos.py`. The other files are framework wiring, so
anything that differs between the Prefect and Dagster versions is that framework's opinion.

## Setup

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is pinned because Prefect and Dagster
do not support 3.14 yet; uv downloads it.

```sh
uv sync
cp .env.example .env
```

Fill in `.env`:

- `NASA_API_KEY`: free, instant, from https://api.nasa.gov. `DEMO_KEY` works but is capped at
  50 requests a day, which a month-long backfill will exceed.
- `MOTHERDUCK_TOKEN`: from https://app.motherduck.com, Settings, Access Tokens. Free tier is 10 GB.

## Step 1: run the core by hand

```sh
uv run --env-file .env python -m core.neos 2026-09-30            # -> ./neos.duckdb
uv run --env-file .env python -m core.neos 2026-09-30 md:neos    # -> MotherDuck
```

Prints the rows written and the three closest approaches. Running the same day twice appends
duplicate rows on purpose; preventing that is the job of the orchestrators in later steps.

## Step 2: Prefect

### Run the flow locally

```sh
uv run --env-file .env python -m flows.neo_flow 2026-10-01
```

Prefect starts a temporary local API server, runs `fetch -> transform -> load`, and stops it.
`fetch` retries three times with backoff. Set `NEOS_DB_PATH=md:neos` to write to MotherDuck
instead of the local file. A `database is locked` line from the temporary server's telemetry
is harmless.

### One-time Prefect Cloud setup

From your own terminal, since it opens a browser:

```sh
uv run prefect cloud login
```

Then:

```sh
# the serverless pool the Hobby tier provides (500 minutes a month)
uv run prefect work-pool create managed --type prefect:managed

# secrets the managed run needs, so it never sees .env
uv run --env-file .env python -c "
from prefect.blocks.system import Secret; import os
Secret(value=os.environ['NASA_API_KEY']).save('nasa-api-key', overwrite=True)
Secret(value=os.environ['MOTHERDUCK_TOKEN']).save('motherduck-token', overwrite=True)"
```

And in the GitHub repo, three Actions secrets: `PREFECT_API_KEY` (from Prefect Cloud, API keys),
`PREFECT_API_URL` (the URL `uv run prefect config view` prints after login), and
`MOTHERDUCK_API_KEY`. The workflow copies the MotherDuck key into the `motherduck-token` Secret
block on every deploy, so you only need `MOTHERDUCK_TOKEN` in `.env` for local runs.

### Deploying

Push to `main`. The workflow runs `prefect deploy --all`, which registers `neo-flow/daily`
against `prefect.yaml`. To do it by hand instead: `uv run prefect deploy --all`.

### What happens on each scheduled run

06:00 America/New_York, set in `prefect.yaml`:

1. Prefect Cloud creates a flow run from the schedule.
2. The managed pool starts a fresh container for it.
3. The container clones this repo at `main` (the `pull` step), installs `pip_packages`, and
   imports `flows/neo_flow.py:neo_flow`.
4. `fetch` loads the NASA key from the `nasa-api-key` Secret block and calls the feed.
5. `load` loads the MotherDuck token from the `motherduck-token` block and inserts into `md:neos`.
6. The container reports state and logs to Prefect Cloud and is discarded.

Trigger a run without waiting:

```sh
uv run prefect deployment run 'neo-flow/daily' --param day=2026-10-02
```

### Failure email

An automation named `neo-flow failed -> email` sends a mail (via the `failure-email` block) when
a run of `neo-flow/daily` enters Failed or Crashed. It was created with the Python client; the
Automations page in Prefect Cloud shows and edits it. Test it with a bad input:

```sh
uv run prefect deployment run 'neo-flow/daily' --param day=not-a-date --watch
```

## Step 3: access from Claude Code

`.mcp.json` wires two existing MCP servers into Claude Code. No tool code in this repo:

- `prefect`: Prefect's official server (`uvx --from prefect-mcp prefect-mcp-server`), built on
  FastMCP. Reads your active Prefect profile, so `prefect cloud login` is all it needs. Fifteen
  read-only tools: deployments, flow runs, logs, work pools, automations, events, docs search.
- `motherduck`: MotherDuck's official server, started through `scripts/motherduck-mcp.sh`, which
  loads `.env` first because Claude Code does not. Needs `MOTHERDUCK_TOKEN` in `.env`. Opened
  read-only on `md:neos`.

Restart Claude Code in this directory and approve the two project servers when prompted. Then:

- "Did last night's neo-flow run pass? Show me the logs if not."
- "Which asteroid passed closest to Earth this week, and how big was it?"

To read the FastMCP side, the Prefect server's source is at github.com/PrefectHQ/prefect-mcp-server.

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
- [x] Step 2b: repo on GitHub, Actions workflow deploys on push, `neo-flow/daily` registered on the managed pool
- [x] Step 2c: MotherDuck token synced into a Secret block by the workflow, first managed run wrote rows to `md:neos`
- [x] Step 2d: failure automation emails you
- [x] Step 3: MCP servers in `.mcp.json` (MotherDuck one needs `MOTHERDUCK_TOKEN` in `.env`)
- [ ] Step 4: Dagster assets and check
- [ ] Step 5: capstone

## Deliberately left out

- **Any theme.** Asteroids are here because the API is free, keyless for the first few runs, and date-addressable.
- **A local worker.** The Hobby tier does not allow worker-based pools, so there is nothing to keep running on your laptop.
- **Dagster+.** No free tier, and its MCP server only talks to Dagster+. Local `dagster dev` is enough.
- **A local LLM.** Claude Code is the one asking the questions.
