"""Simple Lecture 4 ETL example for Tartu air-quality data.

This script keeps the ETL idea in one file on purpose:
1. extract one date window from the API
2. transform the raw rows into hourly rows
3. load them into one PostgreSQL table

It is meant to be read before the more advanced CLI ETL.
"""

from argparse import ArgumentParser
from datetime import datetime, timedelta
import os
from pathlib import Path

try:
    import psycopg2
    import requests
except ImportError as exc:  # pragma: no cover - runtime environment concern
    raise RuntimeError(
        "Run this script inside the project virtualenv: .venv/bin/python ..."
    ) from exc


# --- Fixed lecture example values -------------------------------------------

DEFAULT_API_BASE_URL = "https://www.ohuseire.ee/api"
DEFAULT_API_LOCALE = "en"

STATION_ID = 8
STATION_NAME = "Tartu"
STATION_TYPE = "INDICATOR"

# These are the air-quality indicators currently exposed for Tartu station 8.
# We hard-code them here so students can focus on ETL first.
# The API's historical timestamp bug also follows this station-specific order.
API_INDICATOR_IDS = (21, 23, 4, 3, 1, 6, 37, 41, 66,)

INDICATOR_COLUMNS = {
    21: "pm10",
    23: "pm2_5",
    4: "co",
    3: "no2",
    1: "so2",
    6: "o3",
    37: "wd10",
    41: "ws10",
    66: "temp",
}

TARGET_SCHEMA = "l4_simple"
TARGET_TABLE = "air_quality_station_8_hourly"
TARGET_RELATION = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
TARGET_COLUMNS = tuple(INDICATOR_COLUMNS.values())


# --- Small setup helpers ----------------------------------------------------

def load_env_file(path):
    """Load KEY=VALUE pairs from `.env` when the file exists."""

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


def parse_args():
    """Read the date window and load mode from the command line."""

    parser = ArgumentParser(
        description="Simple Lecture 4 ETL for Tartu air-quality station 8"
    )
    parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--load-mode",
        required=True,
        choices=("replace", "update"),
        help="replace = truncate and reload, update = upsert by station_id + observed_at",
    )
    return parser.parse_args()


def api_base_url():
    """Return the API base URL, allowing a local `.env` override."""

    return os.getenv("AIRVIRO_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/")


def api_locale():
    """Return the API locale, allowing a local `.env` override."""

    value = os.getenv("AIRVIRO_API_LOCALE", DEFAULT_API_LOCALE).strip()
    return value or DEFAULT_API_LOCALE


def format_api_date(value):
    """Convert `YYYY-MM-DD` dates into the API's `dd.MM.yyyy` style."""

    return value.strftime("%d.%m.%Y")


# --- Extract ----------------------------------------------------------------

def extract(from_date, to_date):
    """Download one raw monitoring window from the Ohuseire API."""

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    url = f"{api_base_url()}/monitoring/{api_locale()}"
    params = {
        "stations": str(STATION_ID),
        "type": STATION_TYPE,
        "range": f"{format_api_date(start)},{format_api_date(end)}",
        "indicators": ",".join(str(indicator_id) for indicator_id in API_INDICATOR_IDS),
    }

    response = requests.get(url, params=params, timeout=45)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Monitoring API returned an unexpected payload.")

    return data


# --- Transform --------------------------------------------------------------

def parse_number(raw_value):
    """Turn API numbers like `0,5` or `3 061` into Python floats."""

    value = "" if raw_value is None else str(raw_value).strip().strip('"')
    if value in {"", "-", "NA", "N/A", "null", "NULL"}:
        return None

    compact = value.replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    compact = compact.replace(",", ".")
    return float(compact)


def parse_measurements(raw_rows):
    """Read the raw API rows into a cleaner intermediate list."""

    measurements = []

    for index, item in enumerate(raw_rows, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Monitoring row {index} is not an object.")

        station_id = int(item.get("station"))
        if station_id != STATION_ID:
            raise ValueError(
                f"API returned station {station_id} instead of station {STATION_ID}."
            )

        indicator_id = int(item.get("indicator"))
        if indicator_id not in INDICATOR_COLUMNS:
            continue

        measurements.append(
            {
                "station_id": station_id,
                "indicator_id": indicator_id,
                "observed_at": datetime.strptime(
                    str(item.get("measured") or ""),
                    "%Y-%m-%d %H:%M:%S",
                ),
                "value": parse_number(item.get("value")),
            }
        )

    return measurements


def needs_historical_timestamp_fix(measurements, from_date, to_date):
    """Detect older historical windows whose timestamps come back shifted."""

    if not measurements:
        return False

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    expected_hours = ((end - start).days + 1) * 24
    actual_hours = {measurement["observed_at"] for measurement in measurements}
    return len(actual_hours) > expected_hours


def fix_historical_timestamps(measurements):
    """Shift older historical rows back to clean hourly timestamps.

    The API sometimes returns older data with timestamps staggered by the
    position of the indicator in the station's API order. For the simple
    lecture script, we keep that order in one explicit constant.
    """

    indicator_offsets = {
        indicator_id: index + 1
        for index, indicator_id in enumerate(API_INDICATOR_IDS)
    }
    fixed_measurements = []

    for measurement in measurements:
        offset_hours = indicator_offsets[measurement["indicator_id"]]
        fixed_measurements.append(
            {
                **measurement,
                "observed_at": measurement["observed_at"] - timedelta(hours=offset_hours),
            }
        )

    return fixed_measurements


def pivot_hourly_rows(measurements):
    """Turn long-form measurements into one wide row per hour."""

    rows_by_timestamp = {}

    for measurement in measurements:
        observed_at = measurement["observed_at"]
        row = rows_by_timestamp.setdefault(
            observed_at,
            {
                "station_id": STATION_ID,
                "observed_at": observed_at,
                **{column: None for column in TARGET_COLUMNS},
            },
        )
        column_name = INDICATOR_COLUMNS[measurement["indicator_id"]]
        row[column_name] = measurement["value"]

    rows = list(rows_by_timestamp.values())
    rows.sort(key=lambda row: row["observed_at"])
    return rows


def transform(raw_rows, from_date, to_date):
    """Clean the API rows and shape them into hourly table rows."""

    measurements = parse_measurements(raw_rows)

    if needs_historical_timestamp_fix(measurements, from_date, to_date):
        measurements = fix_historical_timestamps(measurements)

    return pivot_hourly_rows(measurements)


# --- Load -------------------------------------------------------------------

def build_db_config():
    """Read warehouse connection settings from environment variables."""

    return {
        "host": os.getenv("WAREHOUSE_DB_HOST", "postgres"),
        "port": int(os.getenv("WAREHOUSE_DB_PORT", "5432")),
        "dbname": os.getenv("WAREHOUSE_DB_NAME", "warehouse"),
        "user": os.getenv("WAREHOUSE_DB_USER", "warehouse"),
        "password": os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse"),
    }


def ensure_target_table(cursor):
    """Create the target schema and table when they do not exist yet."""

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_RELATION} (
          station_id integer NOT NULL,
          observed_at timestamp without time zone NOT NULL,
          so2 double precision,
          no2 double precision,
          co double precision,
          o3 double precision,
          pm10 double precision,
          pm2_5 double precision,
          temp double precision,
          wd10 double precision,
          ws10 double precision,
          loaded_at timestamp with time zone NOT NULL DEFAULT now(),
          CONSTRAINT air_quality_station_8_hourly_pk
            PRIMARY KEY (station_id, observed_at)
        )
        """
    )


def load(rows, load_mode):
    """Write the transformed rows into PostgreSQL."""

    payload = [
        (
            row["station_id"],
            row["observed_at"],
            row["so2"],
            row["no2"],
            row["co"],
            row["o3"],
            row["pm10"],
            row["pm2_5"],
            row["temp"],
            row["wd10"],
            row["ws10"],
        )
        for row in rows
    ]

    conn = psycopg2.connect(**build_db_config())

    try:
        with conn.cursor() as cursor:
            ensure_target_table(cursor)

            if load_mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {TARGET_RELATION}")

            cursor.executemany(
                f"""
                INSERT INTO {TARGET_RELATION} (
                  station_id,
                  observed_at,
                  so2,
                  no2,
                  co,
                  o3,
                  pm10,
                  pm2_5,
                  temp,
                  wd10,
                  ws10
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, observed_at)
                DO UPDATE SET
                  so2 = EXCLUDED.so2,
                  no2 = EXCLUDED.no2,
                  co = EXCLUDED.co,
                  o3 = EXCLUDED.o3,
                  pm10 = EXCLUDED.pm10,
                  pm2_5 = EXCLUDED.pm2_5,
                  temp = EXCLUDED.temp,
                  wd10 = EXCLUDED.wd10,
                  ws10 = EXCLUDED.ws10,
                  loaded_at = now()
                """,
                payload,
            )

        conn.commit()
    finally:
        conn.close()

    return len(payload)


# --- Main -------------------------------------------------------------------

def main():
    """Run the simple ETL from the command line."""

    load_env_file(Path(".env"))
    args = parse_args()

    print("=== Lecture 4 Simple ETL ===")
    print(f"Source API: {api_base_url()}")
    print(f"Source: {STATION_NAME} air-quality station {STATION_ID}")
    print(f"Target table: {TARGET_RELATION}")
    print(f"Window: {args.from_date}..{args.to_date}")
    print(f"Load mode: {args.load_mode}")

    raw_rows = extract(args.from_date, args.to_date)
    print(f"Extracted: {len(raw_rows)} raw API rows")

    rows = transform(raw_rows, args.from_date, args.to_date)
    print(f"Transformed: {len(rows)} hourly rows")

    loaded = load(rows, args.load_mode)
    print(f"Loaded: {loaded} rows")

    if rows:
        print(f"First observed_at: {rows[0]['observed_at']}")
        print(f"Last observed_at: {rows[-1]['observed_at']}")

    print("=== Simple ETL Complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
