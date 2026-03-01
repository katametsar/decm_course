# Airflow + dbt Pipeline Notes

This document tracks the SQL-first transformation setup before DAG orchestration is added.

## Current State

- Extraction/loading is handled by Python ETL (`etl/airviro`).
- Transformations are handled by dbt (`dbt/`).
- dbt is executed inside `airflow-scheduler` so course runtime is consistent.

## Run Order (Manual)

1. Start stack:

```bash
make up-all
```

2. Load/refresh raw data:

```bash
make etl-backfill-2020-2025
```

3. Build transformation layer:

```bash
make dbt-build
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

## Next Step

Add Airflow DAGs that orchestrate:
1. ETL extraction/load by date window
2. `dbt seed`
3. `dbt run`
4. `dbt test`

