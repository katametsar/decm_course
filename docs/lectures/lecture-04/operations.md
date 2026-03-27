# Airviro ETL Notes (Lecture 4 Starter)

For the student-facing lecture flow and learning outcomes, start with:
- [Lecture 4 Overview](./README.md)

## Overview

Lecture 4 now has two ETL layers in the same warehouse database:

1. Simple tutorial ETL
   - relation: `warehouse.l4_simple.air_quality_station_8_hourly`
   - source: Tartu air-quality station `8`
   - goal: understand ETL stages with one readable script

2. Advanced Lecture 4 ETL
   - raw layer: `warehouse.l4_raw.*`
   - serving layer: `warehouse.l4_mart.*`
   - sources:
     - Tartu air-quality station `8`
     - Tartu pollen station `25`
   - goal: show how a more robust ETL design supports validation, reruns, and Superset-ready views

This lecture intentionally avoids air-quality station `19`.
That station can be introduced later together with stronger dimensional modeling in Lecture 5.

## Source API

Lecture 4 no longer uses the retired `airviro.klab.ee/station/csv` endpoint.

The current lecture source is the Ohuseire JSON API:
- station metadata: `/api/station/{locale}`
- indicator metadata: `/api/indicator/{locale}`
- monitoring rows: `/api/monitoring/{locale}`

Lecture 4 keeps the package name `etl.airviro`, but the live source contract is now the Ohuseire API.

## Why This Design

The simple ETL teaches the basic ETL loop clearly:
- discover one source;
- fetch one monitoring window;
- normalize it;
- load one table.

The advanced ETL demonstrates practical design patterns:
- discover source IDs and indicator IDs from metadata endpoints;
- bounded extraction windows;
- guarded extraction around known unsafe source dates;
- retries and split-on-failure behavior;
- normalization of older staggered historical timestamps;
- long-form raw storage;
- idempotent upserts on natural keys;
- audit logging and status reporting;
- curated serving views and dimensions.

## CLI Commands

Run from repo root:

```bash
.venv/bin/python -m etl.airviro.cli bootstrap-db
.venv/bin/python -m etl.airviro.cli run --from 2026-03-10 --to 2026-03-12 --source-key air_quality_station_8 --source-key pollen_station_25
.venv/bin/python -m etl.airviro.cli backfill --from 2020-01-01 --source-key air_quality_station_8 --source-key pollen_station_25
.venv/bin/python -m etl.airviro.cli warehouse-status
```

Verbose progress:

```bash
.venv/bin/python -m etl.airviro.cli run --from 2026-03-10 --to 2026-03-12 --source-key air_quality_station_8 --source-key pollen_station_25 --verbose
```

`--verbose` prints source/window progress, retries, split events, coverage warnings, and cumulative counts to stderr while keeping the final JSON summary on stdout.

Warehouse status can also be exported as JSON:

```bash
.venv/bin/python -m etl.airviro.cli warehouse-status --json
```

Dry-run validation without DB writes:

```bash
.venv/bin/python -m etl.airviro.cli run --from 2026-03-10 --to 2026-03-12 --source-key air_quality_station_8 --source-key pollen_station_25 --dry-run
```

Bootstrap note:
- Lecture 4 bootstrap intentionally avoids schema-migration logic.
- If the Lecture 4 raw/mart structure changes during lecture preparation, reset `l4_raw` and `l4_mart`, then rerun `make etl-bootstrap`.

## Lecture 4 Schemas and Relations

### Simple ETL

- schema: `l4_simple`
- table: `air_quality_station_8_hourly`

### Advanced ETL Raw Layer

- schema: `l4_raw`
- table: `airviro_measurement`
- table: `airviro_ingestion_audit`
- table: `pipeline_watermark`

### Advanced ETL Serving Layer

- schema: `l4_mart`
- view: `v_air_quality_hourly_station_8`
- view: `v_pollen_daily_station_25`
- view: `v_airviro_measurements_long`

Dimensions:
- `l4_mart.dim_datetime_hour`
- `l4_mart.dim_indicator`
- `l4_mart.dim_wind_direction`

## Where the Advanced Dimensions Come From

- `l4_mart.dim_indicator`
  - refreshed from distinct indicator values loaded into `l4_raw.airviro_measurement`
  - indicator names originate from the indicator metadata API

- `l4_mart.dim_datetime_hour`
  - refreshed from distinct timestamps loaded into `l4_raw.airviro_measurement`
  - includes readable labels `month_name`, `day_name`
  - includes Superset-friendly sortable labels `month_short`, `day_short` with leading-space padding for chronological alphabetic order

- `l4_mart.dim_wind_direction`
  - static bootstrap lookup created by the Lecture 4 schema SQL

## Source Configuration in `.env`

- `AIRVIRO_BASE_URL` (Lecture 4 default `https://www.ohuseire.ee/api`)
- `AIRVIRO_API_LOCALE` (Lecture 4 default `en`)
- `AIRVIRO_AIR_STATION_IDS` (Lecture 4 default `8`)
- `AIRVIRO_POLLEN_STATION_IDS` (Lecture 4 default `25`)
- `AIRVIRO_RAW_SCHEMA` (Lecture 4 default `l4_raw`)
- `AIRVIRO_MART_SCHEMA` (Lecture 4 default `l4_mart`)

## API Caveats Handled in the Advanced ETL

The current API is better structured than the old CSV route, but it still has a few operational wrinkles:

1. Wide windows can still be risky
   - the advanced extractor starts with bounded windows;
   - retries transient failures;
   - splits failing windows recursively when needed.

2. Older historical monitoring rows can be timestamp-staggered
   - for some historical windows, indicator rows arrive shifted by indicator order;
   - the advanced transform normalizes those rows back to clean hourly or daily timestamps before loading.

3. The `2025-10-26` station-8 air-quality day is treated as unsafe
   - live checks showed a DST duplicate hour and staggered early-hour timestamp behavior in the same response;
   - the advanced CLI ETL skips that one date during window selection and warns the user;
   - a request that only covers `2025-10-26` returns no station-8 air-quality rows by design.

4. Successful responses still need validation
   - the advanced ETL emits a warning when returned timestamps do not fully cover the requested window.

## Source Links

- Ohuseire homepage:
  - <https://www.ohuseire.ee/>

- Station metadata:
  - <https://www.ohuseire.ee/api/station/en>

- Indicator metadata:
  - <https://www.ohuseire.ee/api/indicator/en?type=INDICATOR>
  - <https://www.ohuseire.ee/api/indicator/en?type=POLLEN>

- Monitoring examples:
  - <https://www.ohuseire.ee/api/monitoring/en?stations=8&type=INDICATOR&range=10.03.2026%2C12.03.2026>
  - <https://www.ohuseire.ee/api/monitoring/en?stations=25&type=POLLEN&range=10.03.2026%2C12.03.2026>

- Historical example used for validation of the staggered-timestamp normalization:
  - <https://www.ohuseire.ee/api/monitoring/en?stations=8&type=INDICATOR&range=01.05.2025%2C03.05.2025>
  - <https://www.ohuseire.ee/api/monitoring/en?stations=25&type=POLLEN&range=01.05.2025%2C03.05.2025>
