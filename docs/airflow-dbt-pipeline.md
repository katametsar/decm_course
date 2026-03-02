# Airflow + dbt Pipeline Notes

This document describes the current Airflow + dbt orchestration setup.

## Current State

- Extraction/loading is handled by Python ETL (`etl/airviro`).
- Transformations are handled by dbt (`dbt/`).
- dbt is executed inside `airflow-scheduler` so course runtime is consistent.
- Airflow DAGs orchestrate both ETL and dbt:
  - `airviro_incremental` (scheduled)
  - `airviro_backfill` (manual)

## DAG Workflows

### Incremental (scheduled hourly)

1. Ensure prerequisites:
   `raw.pipeline_watermark` exists and ETL schema is bootstrapped.
2. Read watermark for `airviro_incremental`.
3. Compute next date chunk (`AIRFLOW_AIRVIRO_INCREMENTAL_MAX_DAYS`).
4. Run ETL for that chunk.
5. Run `dbt seed`, `dbt run`, `dbt test`.
6. Advance watermark only on full success, capped to `today - 1` (UTC).

Notes:
- Watermark represents the last fully closed day, not "latest loaded timestamp".
- Current day is intentionally reloaded every hourly run so newly published intra-day source rows are ingested.

### Backfill (manual)

1. Accept run params: `start_date`, `end_date`, `chunk_days`, `advance_watermark`.
2. Split range into chunks.
3. Run ETL for each chunk in sequence.
4. Run `dbt seed`, `dbt run`, `dbt test`.
5. Optionally advance incremental watermark using `GREATEST(existing, candidate_date)`, where `candidate_date = min(end_date, today - 1)`.

## Commands

Start stack:

```bash
make up-all
```

List DAGs/runs:

```bash
make airflow-list-dags
make airflow-list-runs DAG_ID=airviro_incremental
```

Trigger incremental:

```bash
make airflow-trigger-incremental
```

Trigger backfill:

```bash
make airflow-trigger-backfill BACKFILL_START=2020-01-01 BACKFILL_END=2025-12-31 BACKFILL_CHUNK_DAYS=31
```

## Data Contracts

- Raw source table: `raw.airviro_measurement`
- dbt outputs (schema `mart`):
  - `dim_indicator`
  - `dim_datetime_hour`
  - `dim_wind_direction`
  - `v_airviro_measurements_long`
  - `v_air_quality_hourly`
  - `v_pollen_daily`

## Operational Notes

- Incremental catch-up is bounded per run by `AIRFLOW_AIRVIRO_INCREMENTAL_MAX_DAYS`.
- If the stack is down for a period, subsequent scheduled runs continue from watermark.
- Once watermark reaches yesterday, incremental keeps refreshing today's date each hour.
- Keep `catchup=False` to avoid scheduler creating historical run backlog; watermark handles data backlog instead.
