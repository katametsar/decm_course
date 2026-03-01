# Airviro Pipeline V2: Airflow Orchestration + dbt SQL Transformations

This ExecPlan is a living document. Update `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` as work advances.

Reference: `PLANS.md` (repository root) for standards.

## Purpose / Big Picture

Move the current ETL architecture toward a production-style teaching pattern:
- keep Python focused on extraction/loading into raw tables;
- move transformations to dbt models (SQL-first);
- orchestrate incremental loads and backfills with Airflow DAGs.

Students should be able to start Docker Compose, run one command or enable one DAG, and get updated Superset-ready marts with minimal manual intervention.

## Student Learning Impact

- Lecture 2 becomes clearer: ingestion and modeling are separated.
- Lecture 3 becomes realistic: students see Airflow orchestration + dbt dependency graph + tests.
- Students learn two distinct backfill patterns:
  - scheduled incremental with automatic gap catch-up after downtime;
  - explicit historical backfill runs (2020 -> present).

## Scope

In scope:
- Introduce a dbt project for Airviro transformations and tests.
- Add Airflow DAGs for incremental updates and parameterized backfill.
- Add pipeline state tracking (watermark) for robust stop/start behavior.
- Integrate Airflow runtime dependencies (dbt + ETL code availability).
- Update docs and Make targets for student workflows.

Out of scope:
- Replacing Airviro extraction with pure SQL (source is HTTP CSV API, still needs Python extraction).
- Advanced deployment concerns (Kubernetes, CeleryExecutor, external secrets manager).
- Superset dashboard JSON export/import automation (can be a follow-up).

## Progress

- [x] Investigate current state and constraints
- [x] Finalize target architecture and dependency strategy
- [x] Implement dbt project and model/test structure
- [x] Add dbt execution path in compose and Makefile
- [x] Implement Airflow DAGs (incremental + backfill)
- [x] Add Airflow DAG docs and trigger helpers
- [x] Run end-to-end validation
- [x] Final review and cleanup

## Surprises & Discoveries

- Discovery: Airflow containers currently mount only `airflow/dags`, not ETL or dbt project files.
  Evidence: `docker-compose.yml` mounts `HOST_WORKSPACE/airflow/dags` to `/opt/airflow/dags` but no `dbt/` or `etl/` paths.

- Discovery: Current transformations are primarily in bootstrap SQL/views, not dbt models.
  Evidence: only `sql/warehouse/airviro_schema.sql` exists under `sql/`; no `dbt/` project directory exists yet.

- Discovery: Existing ETL already supports idempotent range runs and backfill CLI arguments.
  Evidence: `etl/airviro/cli.py` provides `run --from/--to` and `backfill --from/--to`.

- Discovery: Stock `apache/airflow:3.1.7` image in this environment does not include dbt.
  Evidence: `pip show dbt-postgres` and `import dbt` both fail in `airflow-scheduler` container.

- Discovery: Building custom image from repository root is unnecessary for current dependency needs.
  Evidence: custom image builds successfully from `airflow/image` context with local `requirements.txt`.

- Discovery: Installing dbt directly into Airflow's Python environment causes dependency drift risk.
  Evidence: dbt transitive dependencies upgraded OpenTelemetry packages outside Airflow's expected range.

- Discovery: dbt schema config can accidentally create `mart_mart` if both profile schema and model `+schema` are set.
  Evidence: initial dbt run materialized objects in `mart_mart`; fixed by keeping model schema inheritance aligned with profile.

- Discovery: ETL mart-dimension refresh using `ON CONFLICT(<columns>)` can fail against dbt-managed tables without matching unique constraints.
  Evidence: Airflow task logs showed `psycopg2.errors.InvalidColumnReference` for `mart.dim_datetime_hour` conflict target; fixed by constraint-agnostic idempotent refresh SQL (`ON CONFLICT DO NOTHING` + `WHERE NOT EXISTS`/`UPDATE`).

## Decision Log

- Decision: Use dbt for all transformation layers (staging, dimensions, marts), keep Python only for extract/load.
  Rationale: maximizes SQL learning value while preserving practical API ingestion reliability.
  Date: 2026-02-28

- Decision: Use watermark-driven incremental DAG with `catchup=False` for default student workflow.
  Rationale: prevents massive scheduler backlog after downtime while still auto-processing missed dates on next startup.
  Date: 2026-02-28

- Decision: Provide a separate manual backfill DAG with explicit date params.
  Rationale: makes historical replay a deliberate teaching action and avoids accidental heavy runs.
  Date: 2026-02-28

- Decision: Use a custom Airflow image for deterministic dependencies (`dbt-postgres`, ETL requirements).
  Rationale: avoids runtime pip installs and keeps runs reproducible across students.
  Date: 2026-02-28

- Decision: Keep custom Airflow Docker build assets under `airflow/image/` and build via Compose profile services.
  Rationale: keeps project root clean and makes dependency ownership obvious for students.
  Date: 2026-03-01

- Decision: Install dbt dependencies into an isolated virtualenv inside the Airflow image.
  Rationale: prevents dbt package transitive upgrades from breaking Airflow runtime dependencies.
  Date: 2026-03-01

## Outcomes & Retrospective

Planned outcomes:
- dbt project added and runnable from both devcontainer and Airflow tasks.
- Airflow DAGs added for incremental and backfill operation.
- Gap catch-up works after Compose stop/start.

Current status:
- dbt project scaffolded (`dbt/`) with seeds, staging, marts, and tests.
- Airflow services mount `dbt/` and `etl/` folders for orchestration-ready runtime.
- Make targets added: `dbt-debug`, `dbt-seed`, `dbt-run`, `dbt-test`, `dbt-build`.
- Airflow DAGs added:
  - `airviro_incremental` with watermark-controlled chunk processing.
  - `airviro_backfill` with parameterized range/chunk execution.
- End-to-end checks completed:
  - `make dbt-build` passes (`seed`, `run`, `test`).
  - Fresh manual DAG runs for both `airviro_incremental` and `airviro_backfill` completed in `success` state.

Deferred items (if needed):
- Airflow Dataset/Event-based orchestration.
- dbt docs site publishing.
- alerting (Slack/email) for failed runs.

## Context and Orientation

Current key paths:
- `docker-compose.yml`
- `airflow/dags/`
- `etl/airviro/cli.py`
- `etl/airviro/pipeline.py`
- `sql/warehouse/airviro_schema.sql`
- `Makefile`
- `README.md`

Planned new paths:
- `dbt/dbt_project.yml`
- `dbt/models/**`
- `dbt/seeds/dim_wind_direction_seed.csv`
- `dbt/tests/**` (or schema tests in model YAML)
- `airflow/dags/airviro_incremental.py`
- `airflow/dags/airviro_backfill.py`
- `airflow/image/Dockerfile` (custom image with dbt + ETL deps)
- `airflow/image/requirements.txt`
- `docs/airflow-dbt-pipeline.md`

Environment and services:
- Shared Postgres service: `postgres` (warehouse DB already provisioned).
- Airflow services: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`.
- Superset reads curated mart objects from `warehouse`.

## Plan of Work

### Phase 1: Baseline and Design

- Confirm current raw-table contract remains stable (`raw.airviro_measurement` natural keys).
- Define dbt layer boundaries:
  - `stg_*` for canonicalized source fields.
  - `dim_*` and `fct/mart_*` for serving.
- Decide runtime layout:
  - custom Airflow image with dbt installed;
  - mount dbt project and DAG code for easy iteration.

### Phase 2: dbt Implementation (SQL-first)

- Create dbt project with Postgres profile driven by existing env vars.
- Implement models:
  - `stg_airviro_measurement`
  - `dim_indicator`
  - `dim_datetime_hour`
  - `fct_air_quality_hourly` (or mart equivalent)
  - `fct_pollen_daily` (or mart equivalent)
- Move wind-direction lookup into dbt seed and reference it in marts.
- Add dbt tests:
  - uniqueness and not-null on keys;
  - accepted values for `source_type`;
  - relationships between facts and dimensions.

### Phase 3: Airflow DAG Implementation

- Add metadata table for state/watermark (for example `raw.pipeline_watermark`).
- Implement DAG `airviro_incremental`:
  - determine missing date windows from watermark to today;
  - run extraction for each window (idempotent ETL CLI);
  - run `dbt seed`, `dbt run`, `dbt test`;
  - update watermark only after successful dbt/test.
- Implement DAG `airviro_backfill`:
  - accepts `start_date`, `end_date`, `chunk_days`;
  - executes extraction + dbt for selected range;
  - safe to re-run due to upserts/incremental models.

### Phase 4: Integration and Docs

- Wire Airflow image/build and required mounts in `docker-compose.yml`.
- Add Make targets for:
  - dbt local runs (`make dbt-run`, `make dbt-test`);
  - Airflow DAG trigger helpers (`make airflow-trigger-backfill ...`).
- Update `README.md` and add `docs/airflow-dbt-pipeline.md` with copy-paste workflows.

## Concrete Steps

- Run (repo root): `make up-all`
  Expected: Postgres, Superset, and Airflow services are healthy.

- Run (repo root): `make etl-bootstrap`
  Expected: schemas/tables required by raw ingestion exist.

- Run (repo root): `make etl-backfill-2020-2025 VERBOSE=1`
  Expected: raw table contains 2020-2025 measurements.

- Run (repo root, dbt phase): `dbt seed --project-dir dbt --profiles-dir dbt`
  Expected: wind-direction seed loads with 8 rows.

- Run (repo root, dbt phase): `dbt run --project-dir dbt --profiles-dir dbt`
  Expected: staging/dim/mart models build successfully.

- Run (repo root, dbt phase): `dbt test --project-dir dbt --profiles-dir dbt`
  Expected: tests pass, no key integrity regressions.

- Run (Airflow UI/API): trigger `airviro_incremental`
  Expected: missing windows since watermark are processed, then dbt run/test executes.

- Run (Airflow UI/API): trigger `airviro_backfill` with `start_date=2020-01-01`, `end_date=2025-12-31`
  Expected: backfill completes idempotently; rerun does not create duplicates.

## Validation and Acceptance

- Fresh clone + `make up-all` + documented commands works without host installs beyond VS Code/Docker/Git.
- Incremental DAG run updates raw + marts when new source data exists.
- After stopping Compose for multiple days, next incremental run catches up gaps automatically.
- Backfill DAG can rebuild a historical range and is safe to re-run.
- Superset datasets still query expected mart objects with stable column names.
- dbt tests pass after incremental and backfill runs.

## Idempotence and Recovery

- Raw load remains idempotent via natural-key upserts.
- dbt models use incremental/merge-safe strategy where relevant.
- Watermark updates happen only on full success of extraction + dbt run/test.
- Recovery steps:
  - rerun failed DAG run for same logical date/range;
  - if state is inconsistent, reset watermark to last known good date and re-run;
  - full reset path remains `make reset-volumes` + bootstrap/backfill.

## Artifacts and Notes

- Keep operator logs in `airflow-logs` volume.
- Keep ETL run summaries (JSON output) in task logs.
- Keep dbt artifacts (`run_results.json`, `manifest.json`) for troubleshooting.
- Link this plan from `README.md` once implementation starts.

## Interfaces and Dependencies

- Compose services: `postgres`, `airflow-*`, `superset`.
- Key env contracts:
  - `POSTGRES_HOST`, `POSTGRES_PORT`
  - `WAREHOUSE_DB_NAME`, `WAREHOUSE_DB_USER`, `WAREHOUSE_DB_PASSWORD`
  - `AIRFLOW_*` scheduler/runtime settings
  - `AIRVIRO_*` extraction window and timeout controls
- Data contracts:
  - raw source table: `raw.airviro_measurement`
  - modeled outputs: dbt-managed `mart.*` (or `analytics.*`) objects consumed by Superset
  - wind-direction cardinality fixed at 8 sectors (`N, NE, E, SE, S, SW, W, NW`)
