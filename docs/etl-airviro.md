# Airviro ETL Notes (Lecture 2 Starter)

## Overview

This pipeline ingests:
- Air quality measurements from configured Airviro stations (default `8,19`).
- Pollen measurements from configured Airviro stations (default `25`).

It loads normalized long-form measurements into `warehouse.raw.airviro_measurement` and exposes curated Superset-friendly views in `warehouse.mart.*`.

## Why this design

The pipeline follows practical design principles aligned with modern data engineering patterns:
- ingestion in bounded windows with adaptive splitting;
- long-form raw storage + curated serving views;
- idempotent upserts on natural keys;
- quality checks at ingestion boundary;
- explicit audit logging for reruns and backfills.

## Airviro caveat handled

Airviro can fail on wide date windows (observed as HTTP 503). The extractor:
- starts with large windows to minimize request count;
- splits failed windows recursively until successful or minimum window size is reached.

## CLI commands

Run from repo root:

```bash
.venv/bin/python -m etl.airviro.cli bootstrap-db
.venv/bin/python -m etl.airviro.cli run --from 2020-01-01 --to 2025-12-31
.venv/bin/python -m etl.airviro.cli backfill --from 2020-01-01
```

Run only selected sources (useful for onboarding a new station without replaying existing sources):

```bash
.venv/bin/python -m etl.airviro.cli run --from 2020-01-01 --to 2025-12-31 --source-key air_quality_station_19
```

Verbose progress (recommended while teaching/debugging):

```bash
.venv/bin/python -m etl.airviro.cli run --from 2020-01-01 --to 2025-12-31 --verbose
```

`--verbose` prints source/window progress, retries, split events, and cumulative counts to stderr while keeping the final JSON summary on stdout.

Dry-run validation without DB writes:

```bash
.venv/bin/python -m etl.airviro.cli run --from 2025-01-01 --to 2025-01-31 --dry-run
```

Source configuration in `.env`:

- `AIRVIRO_AIR_STATION_IDS` (comma-separated, default `8,19`)
- `AIRVIRO_POLLEN_STATION_IDS` (comma-separated, default `25`)

## Superset serving objects

- `mart.v_air_quality_hourly`
- `mart.v_pollen_daily`
- `mart.v_airviro_measurements_long`

Dimensions:
- `mart.dim_datetime_hour`
- `mart.dim_indicator`
- `mart.dim_wind_direction`

## Precipitation data options for Tartu

### Recommended for course simplicity: Open-Meteo Archive API

Pros:
- free, no API key for non-commercial use,
- straightforward hourly/daily precipitation fields,
- easy to query by latitude/longitude for Tartu.

Tartu coordinates example:
- latitude `58.3776`
- longitude `26.7290`

### Alternative: Estonian Environment Portal (KAIA) open-data files

Pros:
- official Estonian open-data channel.

Tradeoff:
- file-oriented API (metadata + file download), more setup needed for consistent historical ETL compared with Open-Meteo.

## Source links

- Airviro endpoint examples:
  - <https://airviro.klab.ee/station/csv?filter%5Btype%5D=POLLEN&filter%5BcancelSearch%5D=&filter%5BstationId%5D=25&filter%5BdateFrom%5D=01.05.2025&filter%5BdateUntil%5D=31.05.2025&filter%5BsubmitHit%5D=1&filter%5BindicatorIds%5D=>
  - <https://airviro.klab.ee/station/csv?filter%5BstationId%5D=8&filter%5BdateFrom%5D=21.02.2026&filter%5BdateUntil%5D=28.02.2026>
  - <https://airviro.klab.ee/station/csv?filter%5BstationId%5D=19&filter%5BdateFrom%5D=23.02.2026&filter%5BdateUntil%5D=02.03.2026>
- Open-Meteo APIs:
  - Docs: <https://open-meteo.com/en/docs>
  - Historical weather API: <https://open-meteo.com/en/docs/historical-weather-api>
- Estonian Environment Portal open-data API:
  - Overview: <https://www.ilmateenistus.ee/teenused/avaandmete-api/>
  - Swagger: <https://avaandmed.keskkonnaportaal.ee/swagger/index.html>
- Design-pattern article:
  - Publication URL: <https://aws.plainenglish.io/data-engineering-design-patterns-you-must-learn-in-2026-c25b7bd0b9a7>
  - Friend-link variant used for full text review:
    <https://medium.com/@khushbu.shah_661/data-engineering-design-patterns-you-must-learn-in-2026-c25b7bd0b9a7?sk=6e987862791060915725ed9618652cfd>
