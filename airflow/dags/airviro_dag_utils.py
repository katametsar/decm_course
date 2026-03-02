"""Shared helpers for Airviro Airflow DAGs.

These helpers keep DAG files small and make command/database side-effects explicit.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import shlex
import subprocess
from typing import Iterable

import psycopg2

REPO_ROOT = Path("/opt/airflow")
DBT_DIR = REPO_ROOT / "dbt"
PIPELINE_NAME_INCREMENTAL = "airviro_incremental"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def utc_today() -> date:
    """Return today's date in UTC."""

    return datetime.now(timezone.utc).date()


def parse_iso_date(value: str) -> date:
    """Parse YYYY-MM-DD date strings."""

    return datetime.strptime(value, "%Y-%m-%d").date()


def split_date_range(start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split an inclusive date range into fixed-size chunks."""

    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")

    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=chunk_days - 1), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _warehouse_connect():
    """Connect to the warehouse database using shared env vars."""

    return psycopg2.connect(
        host=_env("WAREHOUSE_DB_HOST", "postgres"),
        port=int(_env("WAREHOUSE_DB_PORT", "5432")),
        dbname=_env("WAREHOUSE_DB_NAME", "warehouse"),
        user=_env("WAREHOUSE_DB_USER", "warehouse"),
        password=_env("WAREHOUSE_DB_PASSWORD", "warehouse"),
        connect_timeout=10,
    )


def ensure_watermark_table() -> None:
    """Create watermark state table if it does not exist."""

    create_sql = """
    CREATE TABLE IF NOT EXISTS raw.pipeline_watermark (
      pipeline_name text PRIMARY KEY,
      watermark_date date NOT NULL,
      updated_at timestamp with time zone NOT NULL DEFAULT now()
    );
    """
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()


def get_watermark(pipeline_name: str) -> date | None:
    """Read watermark date for one pipeline."""

    query = """
    SELECT watermark_date
    FROM raw.pipeline_watermark
    WHERE pipeline_name = %s
    """
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_name,))
            row = cursor.fetchone()
    if row is None:
        return None
    return row[0]


def set_watermark(pipeline_name: str, watermark_date: date) -> None:
    """Upsert watermark date."""

    query = """
    INSERT INTO raw.pipeline_watermark (pipeline_name, watermark_date)
    VALUES (%s, %s)
    ON CONFLICT (pipeline_name)
    DO UPDATE SET
      watermark_date = EXCLUDED.watermark_date,
      updated_at = now()
    """
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_name, watermark_date))
        conn.commit()


def set_watermark_greatest(pipeline_name: str, candidate_date: date) -> None:
    """Upsert watermark using the greater of existing/candidate values."""

    query = """
    INSERT INTO raw.pipeline_watermark (pipeline_name, watermark_date)
    VALUES (%s, %s)
    ON CONFLICT (pipeline_name)
    DO UPDATE SET
      watermark_date = GREATEST(raw.pipeline_watermark.watermark_date, EXCLUDED.watermark_date),
      updated_at = now()
    """
    with _warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_name, candidate_date))
        conn.commit()


def _parse_station_ids(
    *,
    csv_env_name: str,
    single_env_name: str,
    default_station_id: int,
) -> list[int]:
    """Parse station-id configuration with backward-compatible env names."""

    raw_csv = os.getenv(csv_env_name, "").strip()
    if raw_csv:
        station_ids: list[int] = []
        seen: set[int] = set()
        for part in raw_csv.split(","):
            token = part.strip()
            if not token:
                continue
            station_id = int(token)
            if station_id in seen:
                continue
            seen.add(station_id)
            station_ids.append(station_id)
        if station_ids:
            return station_ids

    raw_single = os.getenv(single_env_name, "").strip()
    if raw_single:
        return [int(raw_single)]

    return [default_station_id]


def get_configured_sources() -> list[dict[str, object]]:
    """Return source config metadata from env settings."""

    air_station_ids = _parse_station_ids(
        csv_env_name="AIRVIRO_AIR_STATION_IDS",
        single_env_name="AIRVIRO_AIR_STATION_ID",
        default_station_id=8,
    )
    pollen_station_ids = _parse_station_ids(
        csv_env_name="AIRVIRO_POLLEN_STATION_IDS",
        single_env_name="AIRVIRO_POLLEN_STATION_ID",
        default_station_id=25,
    )

    sources: list[dict[str, object]] = []
    for station_id in air_station_ids:
        sources.append(
            {
                "source_key": f"air_quality_station_{station_id}",
                "source_type": "air_quality",
                "station_id": station_id,
            }
        )
    for station_id in pollen_station_ids:
        sources.append(
            {
                "source_key": f"pollen_station_{station_id}",
                "source_type": "pollen",
                "station_id": station_id,
            }
        )
    return sources


def incremental_source_watermark_key(source_key: str) -> str:
    """Build per-source watermark key for incremental orchestration."""

    return f"{PIPELINE_NAME_INCREMENTAL}:{source_key}"


def _run_command(command: list[str], *, cwd: Path) -> None:
    """Run one shell command with explicit logging and strict failure behavior."""

    printable = " ".join(shlex.quote(part) for part in command)
    print(f"[airviro] running: {printable} (cwd={cwd})")
    completed = subprocess.run(command, cwd=cwd, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {printable}")


def run_etl_range(
    start_date: date,
    end_date: date,
    *,
    verbose: bool,
    source_key: str | None = None,
) -> None:
    """Run ETL for one inclusive date range."""

    command = [
        "python",
        "-m",
        "etl.airviro.cli",
        "run",
        "--from",
        start_date.isoformat(),
        "--to",
        end_date.isoformat(),
    ]
    if source_key:
        command.extend(["--source-key", source_key])
    if verbose:
        command.append("--verbose")
    _run_command(command, cwd=REPO_ROOT)


def run_dbt_build() -> None:
    """Run dbt seed/run/test in the mounted dbt project."""

    command_batches: Iterable[list[str]] = (
        ["dbt", "seed", "--project-dir", ".", "--profiles-dir", "."],
        ["dbt", "run", "--project-dir", ".", "--profiles-dir", "."],
        ["dbt", "test", "--project-dir", ".", "--profiles-dir", "."],
    )
    for command in command_batches:
        _run_command(command, cwd=DBT_DIR)


def ensure_etl_schema() -> None:
    """Run ETL bootstrap command to ensure required schemas/tables/views exist."""

    command = ["python", "-m", "etl.airviro.cli", "bootstrap-db"]
    _run_command(command, cwd=REPO_ROOT)
