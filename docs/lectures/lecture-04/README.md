# Lecture 4: Python ETL + Superset Basics

## Audience and Goal

This lecture is for students who completed Lecture 3 and can already run the repository in a devcontainer.

Goal: understand ETL by first reading one small Python script end to end, then compare it with the more advanced CLI-based ETL used elsewhere in the repository.

## Learning Outcomes

After this lecture, students should be able to:

1. Explain `extract`, `transform`, and `load` in a concrete Python example.
2. Run a simple ETL for Tartu air-quality data with a manual date window.
3. Explain the difference between `replace` and `update` load modes.
4. Explain why the first ETL example hard-codes one known source before moving to API-driven discovery.
5. Run the advanced Lecture 4 ETL for Tartu air quality and Tartu pollen.
6. Connect Superset to the warehouse and build first charts from Lecture 4 datasets.
7. Explain why the advanced ETL uses long-form raw storage, dimensions, and curated serving views.

## Lecture 4 Setup

Run from repo root inside the devcontainer:

```bash
make up-superset
make devcontainer-join-course-network
```

Lecture 4 intentionally keeps Airflow out of scope.
Do not run `make up-all` here; Airflow orchestration is introduced in Lecture 5.

Superset:
- URL: <http://localhost:8088>
- username: `admin`
- password: `admin`

## Source API Caution

As of 2026-03-27, the live Ohuseire API shows two different timestamp behaviors for station `8` historical air-quality data:

- one-day checks through 2025-10-25 returned staggered hourly timestamps that need repair;
- one-day checks from 2025-10-27 onward returned clean hourly timestamps;
- the 2025-10-26 daylight-saving transition day is irregular enough that it should be treated as unsafe, not just approximate.

The current simple ETL still decides whether to apply the historical timestamp repair for the whole request window at once. The advanced CLI ETL now adds a small safeguard for `air_quality_station_8`: if a requested range crosses `2025-10-26`, it closes one source window on `2025-10-25`, resumes on `2025-10-27`, skips `2025-10-26`, and warns the user.

Practical guidance for Lecture 4:

- recent windows such as `2026-03-10..2026-03-12` are safe for demos;
- fully older windows can still be loaded with the current repair logic;
- the advanced CLI ETL now skips `2025-10-26` automatically for `air_quality_station_8`;
- avoid using the simple ETL or a manual one-day advanced request for `2025-10-26` until the source behavior is clarified.

The API maintainer has been notified about the timestamp issue and related source quirks. Because the fix would happen on the source side, the API behavior may change without prior warning. If historical responses start looking different, re-run a small validation window before trusting larger backfills.

## Part 1: Simple ETL Tutorial

We start with one small ETL script and one source:
- Tartu air-quality station `8`
- one manually selected date window
- one output table: `l4_simple.air_quality_station_8_hourly`

This keeps the first example small enough to read in one sitting.

### Why Start This Way

The simple ETL is intentionally limited:
- one script instead of multiple modules;
- one source instead of multiple stations;
- one output table instead of raw + marts + audit tables;
- one explicit choice between `replace` and `update`.

That makes it easier to see what each ETL stage does before introducing more robust engineering patterns.

### Run the Simple ETL

Example: load three days of Tartu air-quality data and replace the target table contents.

```bash
.venv/bin/python etl/lecture4_simple_air_quality.py --from 2026-03-10 --to 2026-03-12 --load-mode replace
```

Run the same window again with upsert behavior:

```bash
.venv/bin/python etl/lecture4_simple_air_quality.py --from 2026-03-10 --to 2026-03-12 --load-mode update
```

What the two load modes mean:
- `replace`: truncates `l4_simple.air_quality_station_8_hourly` and reloads the selected window.
- `update`: inserts new rows and updates existing rows with the same `(station_id, observed_at)` key.

### How the Simple ETL Works

The script lives in:
- `etl/lecture4_simple_air_quality.py`

Read it from top to bottom and connect each function to the ETL stages:

1. Fixed lecture constants
   - the top of the file hard-codes the one lecture source:
     - station `8`
     - `type=INDICATOR`
     - the indicator IDs that become `so2`, `pm10`, `wd10`, and the other target columns
     - the fixed API indicator order used when older historical timestamps need repair
   - that is deliberate: the goal of the first script is to teach ETL flow, not metadata discovery

2. Extract
   - calls `/api/monitoring/en`
   - requests one explicit window using:
     - `stations=8`
     - `type=INDICATOR`
     - `range=dd.MM.yyyy,dd.MM.yyyy`

3. Transform
   - parses monitoring JSON rows
   - normalizes API values into Python floats
   - corrects older staggered timestamps when the API returns historical rows in indicator order instead of clean hourly timestamps
   - pivots long-form API rows into one wide hourly row with columns like `so2`, `no2`, `pm10`, `temp`, `wd10`, `ws10`

4. Load
   - creates schema `l4_simple` and table `air_quality_station_8_hourly` if needed
   - either truncates and reloads (`replace`) or upserts (`update`)

### Why the Simple Table Is Useful

`l4_simple.air_quality_station_8_hourly` is easy to explain because:
- one row means one hourly observation for one station;
- students can inspect the columns directly;
- the table shape is convenient for first Superset charts.

That simplicity is helpful for the first ETL walkthrough, but it does not scale as well once we add more sources and more operational requirements.

### A Few API Habits Worth Keeping

Even in this simple script, there are a few good habits worth noticing:

1. Keep the request parameters explicit
   - `stations`, `type`, `range`, and `indicators` are all visible in one place.

2. Use bounded date windows
   - smaller windows are easier to rerun, easier to debug, and less risky if the source behaves strangely.

3. Validate what came back
   - if the API returns old historical rows with odd timestamps, the ETL should notice and normalize them before loading.

4. Add complexity only when it teaches something
   - in the simple script we hard-code one known source;
   - in the advanced ETL we later add metadata discovery, retries, audits, and multiple source types.

## Part 2: Advanced CLI ETL

After the simple script, we move to the repository's more advanced Airviro ETL.

Lecture 4 scope for the advanced ETL:
- Tartu air-quality station `8`
- Tartu pollen station `25`

Lecture 4 advanced warehouse objects are kept separate from later work:
- raw layer: `l4_raw.*`
- serving layer: `l4_mart.*`

### Run the Advanced ETL

Bootstrap the Lecture 4 advanced warehouse objects:

```bash
make etl-bootstrap
```

Run one small live lecture window:

```bash
.venv/bin/python -m etl.airviro.cli run \
  --from 2026-03-10 \
  --to 2026-03-12 \
  --source-key air_quality_station_8 \
  --source-key pollen_station_25 \
  --verbose
```

Check what reached the warehouse:

```bash
make warehouse-status
```

Optional larger historical load for more data:

```bash
make etl-backfill-2020-2025
```

For historical backfills, keep the source API caution above in mind. Do not assume that one wide range crossing late October 2025 will normalize correctly.
The advanced CLI ETL now protects the station-8 air-quality backfill by skipping `2025-10-26` and warning when that date falls inside the requested range.

### What the Advanced ETL Does Better

Compared with the one-file tutorial ETL, the advanced ETL adds:

1. Better source discovery
   - discovers stations and indicators from API metadata;
   - keeps source IDs and indicator meanings out of hard-coded business logic as much as possible.

2. Better extraction behavior
   - bounded date windows;
   - retries for transient failures;
   - split-on-failure logic when the source rejects wide windows.

3. Better transformation behavior
   - keeps the source grain in long form;
   - normalizes older staggered historical responses before loading;
   - produces stable machine-friendly indicator codes.

4. Better observability
   - ingestion audit rows in `l4_raw.airviro_ingestion_audit`;
   - warehouse status reporting;
   - warning messages when returned timestamps do not fully cover the requested window.

5. Better serving layer for BI
   - curated views in `l4_mart.*`;
   - small reusable dimensions for time, indicator metadata, and wind direction.

## Advanced ETL Stages In This Repository

### Discover

- Read `/api/station/en` to find lecture stations and their indicator lists.
- Read `/api/indicator/en?...` to map indicator IDs to stable names and codes.

### Extract

- Pull monitoring JSON from `/api/monitoring/en` in bounded windows.
- Keep `stations`, `type`, `range`, and `indicators` explicit in the request.
- Retry transient failures.
- Split retriable failing windows into smaller windows automatically.

### Transform

- Parse monitoring timestamps from JSON.
- Normalize values into floats.
- Correct older staggered historical rows when necessary.
- Keep normalized raw measurements in long form:
  - one row per `source_type + station_id + observed_at + indicator_code`

### Load

- Upsert to `l4_raw.airviro_measurement`.
- Record ingestion runs in `l4_raw.airviro_ingestion_audit`.
- Refresh serving dimensions in `l4_mart.*`.

### Serve

- Expose Superset-friendly views in `l4_mart.*`.
- Keep air quality and pollen datasets clearly labeled by station in the relation names.

## Dimensional and Ingestion Design Patterns

### Why Long-Form Raw Storage

The current monitoring API already returns one value per indicator and timestamp.

The advanced ETL keeps that same long-form grain in `l4_raw.airviro_measurement` because:
- different sources can still share one raw table;
- new indicators can appear without redesigning the raw table;
- upsert keys remain explicit and stable;
- later dimensional modeling becomes easier.

### Where the Dimensions Come From

Lecture 4 advanced dimensions come from three places:

1. `l4_mart.dim_indicator`
   - built from distinct `(source_type, indicator_code, indicator_name)` values loaded into `l4_raw.airviro_measurement`
   - those names ultimately come from the indicator metadata API

2. `l4_mart.dim_datetime_hour`
   - built from distinct `observed_at` timestamps loaded into `l4_raw.airviro_measurement`

3. `l4_mart.dim_wind_direction`
   - static lookup created by the Lecture 4 bootstrap SQL
   - maps degree ranges into 8 wind sectors (`N`, `NE`, `E`, `SE`, `S`, `SW`, `W`, `NW`)

### Why There Is No Station Dimension Yet

For Lecture 4 we keep the advanced scope narrow:
- air quality is limited to station `8`
- pollen is limited to station `25`

That keeps the first dimensional-model discussion focused on time, indicators, and serving views.

In Lecture 5 we can safely expand to more stations and then introduce `dim_station` as part of a clearer star schema.

## Superset Datasets For Lecture 4

Connect Superset to the warehouse database:
- host: `postgres`
- port: `5432`
- database: `warehouse`
- username: `warehouse`
- password: `warehouse`

Recommended Lecture 4 datasets:

1. Simple ETL table
   - `l4_simple.air_quality_station_8_hourly`

2. Advanced ETL views
   - `l4_mart.v_air_quality_hourly_station_8`
   - `l4_mart.v_pollen_daily_station_25`
   - `l4_mart.v_airviro_measurements_long`

Suggested teaching pattern:
- use the simple table first to show the direct result of one ETL script;
- then switch to the advanced `l4_mart.*` views to show what a better serving layer looks like.

## Code Walkthrough (File + Function Map)

### Simple ETL

`etl/lecture4_simple_air_quality.py`
- `extract`: fetch one monitoring window from the API
- `transform`: normalize rows, fix older historical timestamps when needed, and pivot into a wide hourly table
- `load`: create/load `l4_simple.air_quality_station_8_hourly`
- `main`: tie the ETL stages together

### Advanced CLI Orchestration

`etl/airviro/cli.py`
- `build_parser`: defines `bootstrap-db`, `run`, `backfill`, and `warehouse-status`
- `run_pipeline`: end-to-end flow for selected source(s) and date range
- `build_progress_logger`: verbose extraction progress events
- `main`: command dispatch and top-level error handling

### Runtime Configuration

`etl/airviro/config.py`
- `Settings.from_env`: reads `.env` values into one typed config object
- `candidate_db_hosts`: supports both devcontainer and compose network host resolution

### Extraction + Transformation

`etl/airviro/pipeline.py`
- `fetch_station_catalog`: discovers station metadata from the API
- `fetch_indicator_catalog`: discovers indicator metadata from the API
- `get_source_configs`: expands configured lecture sources into runnable ETL sources
- `date_chunks`: splits date ranges into fixed-size windows
- `fetch_source_window`: fetches monitoring JSON for one source window
- `parse_monitoring_json`: normalizes API rows into warehouse measurement rows
- `build_window_coverage_warning`: checks returned timestamps against the requested window

### Loading + Dimensions

`etl/airviro/db.py`
- `apply_schema`: creates the Lecture 4 raw/mart warehouse objects
- `upsert_measurements`: upserts into `l4_raw.airviro_measurement`
- `refresh_dimensions`: refreshes `dim_indicator` and `dim_datetime_hour`
- `collect_warehouse_status`: reports warehouse health and completeness
