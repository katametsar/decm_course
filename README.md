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
```

For detailed progress logs (source/window/retry/split + cumulative counters):

```bash
make etl-backfill-2020-2025 VERBOSE=1
```

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
  scheduled hourly, processes one date chunk, runs dbt, then advances watermark.
- `airviro_backfill`:
  manual run for a custom historical range, then runs dbt.

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

Backfill without explicit end date uses today's date:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01
```

### Watermark behavior

- Watermark is stored in `raw.pipeline_watermark` under key `airviro_incremental`.
- Incremental DAG reads watermark, processes next chunk, and updates watermark only after ETL + dbt success.
- Backfill DAG can optionally advance the same watermark using `GREATEST(existing, end_date)` to avoid moving it backward.

Superset snippet examples (calculated columns and metrics):
- `superset/snippets.md`

## Environment File

- `.env` is local-only and ignored by git.
- `.env.example` is tracked and should be copied for new environments.
- `make init` copies `.env.example` to `.env`, generates `SUPERSET_SECRET_KEY`, and sets `AIRFLOW_UID`.
- `.env.example` includes `SUPERSET_DB_*`, `AIRFLOW_DB_*`, and `WAREHOUSE_DB_*` to keep naming explicit by purpose.

## Running Outside Devcontainer

If you run `make` directly on the host, `HOST_WORKSPACE` defaults to your current directory.
If you run `docker compose` manually, export `HOST_WORKSPACE` first:

```bash
export HOST_WORKSPACE="$(pwd)"
```
