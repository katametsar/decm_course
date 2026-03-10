# Course Local Stack: Superset + Airflow

This repository is designed for teaching with minimum host setup.

Students need on host:
- VS Code
- Docker Desktop or Docker Engine
- Git

Everything else runs from the devcontainer.
The devcontainer sets `HOST_WORKSPACE` to your host repo path so Docker bind mounts resolve correctly through the mounted Docker socket.

## 1) First Run

From the repo root inside the devcontainer:

```bash
make init
make up-superset
```

Open Superset at <http://localhost:8088>.

Default Superset login (from `.env`):
- username: `admin`
- password: `admin`

## 2) Start Airflow

```bash
make up-airflow
```

Open Airflow at <http://localhost:8080>.

Note:
- Airflow uses a local custom image built from `airflow/image/Dockerfile`.
- Python dependencies for Airflow tasks (for example `dbt-postgres`) are managed in `airflow/image/requirements.txt`.
- dbt is installed in an isolated virtualenv inside the image (`/opt/dbt-venv`) to avoid dependency conflicts with Airflow itself.
- First Airflow startup may take longer while the image is built.

Default Airflow login (from `.env`):
- username: `airflow`
- password: `airflow`

## 3) Start Both Stacks

```bash
make up-all
```

## Compose Profiles

This project uses one `docker-compose.yml` with profiles:
- `superset`: Superset app + Superset Redis + shared Postgres + init
- `airflow`: Airflow core services + shared Postgres + init

Equivalent direct commands:

```bash
docker compose --profile superset up -d
docker compose --profile airflow up -d
docker compose --profile superset --profile airflow up -d
```

## Persistent Data

The stack is persistent by default:
- Named volumes keep Superset metadata and shared Postgres data across restarts.
- `airflow/dags` is bind-mounted from your repo for live DAG editing.
- `dbt/`, `etl/`, and `sql/` are bind-mounted into Airflow containers for DAG runtime commands.
- Airflow `logs`, `config`, and `plugins` use Docker named volumes.
- The same Postgres instance also creates a `warehouse` database for ETL/dbt work.

## Reset / Cleanup

Stop containers only:

```bash
make down
```

Remove containers and volumes (keeps pulled images):

```bash
make reset-volumes
```

Remove containers, volumes, and local images:

```bash
make reset-all
```

## Useful Commands

```bash
make ps
make logs SERVICE=superset
make logs SERVICE=airflow-scheduler
make logs SERVICE=postgres
```

## Lecture Materials

Student-facing lecture notes are in:

- `docs/lectures/index.md`
- Lecture 1 package: `docs/lectures/lecture-01/`

## ETL Starter (Lecture 2)

Ensure the shared stack is up

Optional (recommended): attach the devcontainer to the compose network so ETL can resolve `postgres` directly:

```bash
make devcontainer-join-course-network
```

Then run ETL from the devcontainer:

```bash
make etl-bootstrap
make etl-dry-run
make etl-backfill-2020-2025
make warehouse-status
```

For detailed progress logs (source/window/retry/split + cumulative counters):

```bash
make etl-backfill-2020-2025 VERBOSE=1
```

Warehouse monitoring report (for quick health checks in class):

```bash
make warehouse-status
make warehouse-status-json
```

This report includes:
- warehouse/table connectivity status;
- total rows + date coverage (earliest/latest);
- coverage by source/station;
- indicator-level missing-hour and null-value counts;
- current watermarks and most recent ingestion audit rows.

Backfill to current date (useful later with Airflow orchestration):

```bash
make etl-backfill-2020-today
```

Detailed ETL notes and precipitation-source investigation:
- `docs/etl-airviro.md`

## dbt Starter (Lecture 3)

dbt runs inside `airflow-scheduler` (same runtime used later by Airflow DAG tasks).

```bash
make dbt-debug
make dbt-build
```

You can also run individual steps:

```bash
make dbt-seed
make dbt-run
make dbt-test
```

dbt project files are in:
- `dbt/`

## Airflow DAG Starter (Lecture 3)

Two DAGs are provided:
- `airviro_incremental`:
  scheduled hourly, processes one date chunk per configured source, runs dbt, then advances per-source watermarks.
- `airviro_backfill`:
  manual run for a custom historical range (all or selected sources), then runs dbt.

List DAGs and runs:

```bash
make airflow-list-dags
make airflow-list-runs DAG_ID=airviro_incremental
```

Unpause course DAGs (recommended once):

```bash
make airflow-unpause-dags
```

Trigger incremental run manually:

```bash
make airflow-trigger-incremental
```

Trigger backfill run:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01 BACKFILL_END=2020-12-31 BACKFILL_CHUNK_DAYS=31
```

Backfill one source only (for example onboarding station 19 without replaying others):

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01 BACKFILL_SOURCE_KEYS=air_quality_station_19
```

Backfill without explicit end date uses today's date:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01
```

### Watermark behavior

- Watermark is stored in `raw.pipeline_watermark` with one key per source:
  `airviro_incremental:<source_key>` (for example `airviro_incremental:air_quality_station_19`).
- Watermark tracks the last fully closed day (at most `today - 1` in UTC).
- Incremental DAG reads per-source watermarks, processes next chunks, and updates watermark only after ETL + dbt success.
- Current day is intentionally reloaded each hourly run (idempotent upserts) so intra-day source updates are captured.
- Backfill DAG can optionally advance matching per-source watermarks using `GREATEST(existing, candidate_date)`, where `candidate_date` is capped at `today - 1`.

Superset snippet examples (calculated columns and metrics):
- `superset/snippets.md`

## Environment File

- `.env` is local-only and ignored by git.
- `.env.example` is tracked and should be copied for new environments.
- `make init` copies `.env.example` to `.env`, generates `SUPERSET_SECRET_KEY`, and sets `AIRFLOW_UID`.
- `.env.example` includes `SUPERSET_DB_*`, `AIRFLOW_DB_*`, and `WAREHOUSE_DB_*` to keep naming explicit by purpose.
- Airviro sources are configured with comma-separated station lists:
  `AIRVIRO_AIR_STATION_IDS` and `AIRVIRO_POLLEN_STATION_IDS`.
- If your `.env` was created before this change, add those two variables manually (or recreate `.env` from `.env.example`).

## Running Outside Devcontainer

If you run `make` directly on the host, `HOST_WORKSPACE` defaults to your current directory.
If you run `docker compose` manually, export `HOST_WORKSPACE` first:

```bash
export HOST_WORKSPACE="$(pwd)"
```
