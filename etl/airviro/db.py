"""Database helpers for Airviro ETL loading."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

try:
    import psycopg2
    from psycopg2 import extras
    from psycopg2.extensions import connection as PgConnection
except ImportError as exc:  # pragma: no cover - runtime environment concern
    raise RuntimeError(
        "psycopg2 is required. Run inside the project virtualenv: .venv/bin/python ..."
    ) from exc

from .config import Settings
from .pipeline import MeasurementRow


def connect_warehouse(settings: Settings) -> tuple[PgConnection, str]:
    """Connect to warehouse DB using candidate hosts.

    Returns:
      (connection, selected_host)
    """

    last_error: Exception | None = None
    for host in settings.candidate_db_hosts():
        try:
            conn = psycopg2.connect(
                host=host,
                port=settings.warehouse_db_port,
                dbname=settings.warehouse_db_name,
                user=settings.warehouse_db_user,
                password=settings.warehouse_db_password,
                connect_timeout=5,
            )
            conn.autocommit = False
            return conn, host
        except Exception as exc:  # pragma: no cover - connection failure path
            last_error = exc

    raise RuntimeError(
        f"Unable to connect to warehouse using hosts {settings.candidate_db_hosts()}: {last_error}"
    )


def apply_schema(connection: PgConnection, sql_path: Path) -> None:
    """Apply schema bootstrap SQL."""

    sql_text = sql_path.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql_text)
    connection.commit()


def upsert_measurements(connection: PgConnection, rows: Iterable[MeasurementRow]) -> int:
    """Insert or update normalized measurements."""

    payload = [
        (
            row.source_type,
            row.station_id,
            row.observed_at,
            row.indicator_code,
            row.indicator_name,
            row.value_numeric,
            row.source_row_hash,
        )
        for row in rows
    ]
    if not payload:
        return 0

    query = """
    INSERT INTO raw.airviro_measurement (
      source_type,
      station_id,
      observed_at,
      indicator_code,
      indicator_name,
      value_numeric,
      source_row_hash
    )
    VALUES %s
    ON CONFLICT (source_type, station_id, observed_at, indicator_code)
    DO UPDATE SET
      indicator_name = EXCLUDED.indicator_name,
      value_numeric = EXCLUDED.value_numeric,
      source_row_hash = EXCLUDED.source_row_hash,
      extracted_at = now();
    """

    with connection.cursor() as cursor:
        extras.execute_values(cursor, query, payload, page_size=5000)
    connection.commit()
    return len(payload)


def refresh_dimensions(connection: PgConnection) -> None:
    """Refresh dimension tables from loaded raw facts."""

    refresh_sql = """
    INSERT INTO mart.dim_indicator (source_type, indicator_code, indicator_name)
    SELECT DISTINCT source_type, indicator_code, indicator_name
    FROM raw.airviro_measurement
    ON CONFLICT DO NOTHING;

    UPDATE mart.dim_indicator AS target
    SET indicator_name = source.indicator_name
    FROM (
      SELECT DISTINCT source_type, indicator_code, indicator_name
      FROM raw.airviro_measurement
    ) AS source
    WHERE target.source_type = source.source_type
      AND target.indicator_code = source.indicator_code
      AND target.indicator_name IS DISTINCT FROM source.indicator_name;

    INSERT INTO mart.dim_datetime_hour (
      observed_at,
      date_value,
      year_number,
      quarter_number,
      month_number,
      month_name,
      day_number,
      hour_number,
      iso_week_number,
      day_of_week_number,
      day_name
    )
    SELECT
      source.observed_at,
      source.observed_at::date,
      EXTRACT(YEAR FROM source.observed_at)::int,
      EXTRACT(QUARTER FROM source.observed_at)::int,
      EXTRACT(MONTH FROM source.observed_at)::int,
      TO_CHAR(source.observed_at, 'Month'),
      EXTRACT(DAY FROM source.observed_at)::int,
      EXTRACT(HOUR FROM source.observed_at)::int,
      EXTRACT(WEEK FROM source.observed_at)::int,
      EXTRACT(ISODOW FROM source.observed_at)::int,
      TO_CHAR(source.observed_at, 'Dy')
    FROM (
      SELECT DISTINCT observed_at
      FROM raw.airviro_measurement
    ) AS source
    WHERE NOT EXISTS (
      SELECT 1
      FROM mart.dim_datetime_hour AS target
      WHERE target.observed_at = source.observed_at
    );
    """

    with connection.cursor() as cursor:
        cursor.execute(refresh_sql)
    connection.commit()


def log_ingestion_audit(
    connection: PgConnection,
    *,
    batch_id: str,
    source_type: str,
    window_start: datetime,
    window_end: datetime,
    rows_read: int,
    records_upserted: int,
    duplicate_records: int,
    split_events: int,
    status: str,
    message: str | None = None,
) -> None:
    """Insert one ingestion-audit record."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw.airviro_ingestion_audit (
              batch_id,
              source_type,
              window_start,
              window_end,
              rows_read,
              records_upserted,
              duplicate_records,
              split_events,
              status,
              message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                batch_id,
                source_type,
                window_start,
                window_end,
                rows_read,
                records_upserted,
                duplicate_records,
                split_events,
                status,
                message,
            ),
        )
    connection.commit()
