"""Extraction and transformation logic for Airviro station CSV data."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import csv
import hashlib
import io
import re
import time
from typing import Callable
import unicodedata
from urllib import error, parse, request

from .config import Settings


DATE_COLUMN_CANDIDATES = ("Kuupäev", "Kuupaev", "Date")
ProgressCallback = Callable[[dict[str, object]], None]


class PipelineError(RuntimeError):
    """Base class for expected ETL failures."""


class SourceFetchError(PipelineError):
    """Raised when source extraction fails."""

    def __init__(self, message: str, retriable: bool) -> None:
        super().__init__(message)
        self.retriable = retriable


class DataQualityError(PipelineError):
    """Raised when parsed data fails integrity checks."""


@dataclass(frozen=True)
class SourceConfig:
    """Extraction configuration for one Airviro source."""

    source_key: str
    source_type: str
    station_id: int
    max_window_days: int
    extra_params: dict[str, str]


@dataclass
class SourceRunSummary:
    """Per-source ETL metrics."""

    source_key: str
    source_type: str
    station_id: int
    windows_requested: int = 0
    rows_read: int = 0
    measurements_upserted: int = 0
    duplicate_measurements: int = 0
    split_events: int = 0


@dataclass(frozen=True)
class MeasurementRow:
    """Normalized long-form measurement record."""

    source_type: str
    station_id: int
    observed_at: datetime
    indicator_code: str
    indicator_name: str
    value_numeric: float | None
    source_row_hash: str


def parse_iso_date(raw: str) -> date:
    """Parse yyyy-mm-dd date strings."""

    return datetime.strptime(raw, "%Y-%m-%d").date()


def format_airviro_date(value: date) -> str:
    """Format dates for Airviro query parameters (dd.mm.yyyy)."""

    return value.strftime("%d.%m.%Y")


def date_chunks(start_date: date, end_date: date, max_days: int) -> list[tuple[date, date]]:
    """Split an inclusive date range into fixed-size windows."""

    windows: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=max_days - 1), end_date)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def get_source_configs(settings: Settings) -> list[SourceConfig]:
    """Return source definitions used by the ETL."""

    sources: list[SourceConfig] = []

    for station_id in settings.air_station_ids:
        sources.append(
            SourceConfig(
                source_key=f"air_quality_station_{station_id}",
                source_type="air_quality",
                station_id=station_id,
                max_window_days=settings.air_quality_window_days,
                extra_params={},
            )
        )

    for station_id in settings.pollen_station_ids:
        sources.append(
            SourceConfig(
                source_key=f"pollen_station_{station_id}",
                source_type="pollen",
                station_id=station_id,
                max_window_days=settings.pollen_window_days,
                extra_params={
                    "filter[type]": "POLLEN",
                    "filter[cancelSearch]": "",
                    "filter[submitHit]": "1",
                    "filter[indicatorIds]": "",
                },
            )
        )

    return sources


def fetch_source_window(
    settings: Settings,
    source: SourceConfig,
    window_start: date,
    window_end: date,
    retry_count: int,
    progress: ProgressCallback | None = None,
) -> str:
    """Fetch one raw CSV response for a date window."""

    params: dict[str, str] = {
        "filter[stationId]": str(source.station_id),
        "filter[dateFrom]": format_airviro_date(window_start),
        "filter[dateUntil]": format_airviro_date(window_end),
    }
    params.update(source.extra_params)

    url = f"{settings.airviro_base_url}?{parse.urlencode(params)}"

    for attempt in range(1, retry_count + 1):
        try:
            with request.urlopen(url, timeout=settings.request_timeout_seconds) as response:
                payload = response.read()
            return payload.decode("utf-8-sig")
        except error.HTTPError as exc:
            retriable = exc.code >= 500
            if not retriable or attempt == retry_count:
                if progress is not None:
                    progress(
                        {
                            "event": "fetch_failed",
                            "source_key": source.source_key,
                            "source_type": source.source_type,
                            "window_start": window_start.isoformat(),
                            "window_end": window_end.isoformat(),
                            "attempt": attempt,
                            "retry_count": retry_count,
                            "retriable": retriable,
                            "reason": f"http_{exc.code}",
                        }
                    )
                raise SourceFetchError(
                    f"{source.source_type} request failed ({exc.code}) for {window_start}..{window_end}",
                    retriable=retriable,
                ) from exc
            if progress is not None:
                progress(
                    {
                        "event": "fetch_retry",
                        "source_key": source.source_key,
                        "source_type": source.source_type,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "attempt": attempt,
                        "retry_count": retry_count,
                        "retriable": retriable,
                        "reason": f"http_{exc.code}",
                        "backoff_seconds": 2**attempt,
                    }
                )
        except (error.URLError, TimeoutError) as exc:
            if attempt == retry_count:
                if progress is not None:
                    progress(
                        {
                            "event": "fetch_failed",
                            "source_key": source.source_key,
                            "source_type": source.source_type,
                            "window_start": window_start.isoformat(),
                            "window_end": window_end.isoformat(),
                            "attempt": attempt,
                            "retry_count": retry_count,
                            "retriable": True,
                            "reason": type(exc).__name__,
                        }
                    )
                raise SourceFetchError(
                    f"{source.source_type} request timed out for {window_start}..{window_end}",
                    retriable=True,
                ) from exc
            if progress is not None:
                progress(
                    {
                        "event": "fetch_retry",
                        "source_key": source.source_key,
                        "source_type": source.source_type,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "attempt": attempt,
                        "retry_count": retry_count,
                        "retriable": True,
                        "reason": type(exc).__name__,
                        "backoff_seconds": 2**attempt,
                    }
                )

        # Basic exponential backoff for transient failures.
        time.sleep(2 ** attempt)

    raise SourceFetchError("unreachable retry state", retriable=True)


def normalize_indicator_code(raw_name: str) -> str:
    """Create stable ascii indicator codes from source column names."""

    normalized = unicodedata.normalize("NFKD", raw_name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_name = ascii_name.replace("%", "pct").replace(".", "_")
    ascii_name = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_name).strip("_").lower()
    if not ascii_name:
        return "unknown_indicator"
    if ascii_name[0].isdigit():
        return f"i_{ascii_name}"
    return ascii_name


def parse_localized_numeric(raw_value: str) -> float | None:
    """Parse values like 3 061, 1,8, empty strings and return numeric or null."""

    value = raw_value.strip().strip('"')
    if value in {"", "-", "NA", "N/A", "null", "NULL"}:
        return None

    compact = value.replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    compact = compact.replace(",", ".")
    return float(compact)


def parse_airviro_csv(
    source: SourceConfig,
    csv_text: str,
) -> tuple[list[MeasurementRow], int, int]:
    """Parse source CSV text into normalized long-form measurements.

    Returns:
      records, rows_read, duplicate_measurement_count
    """

    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    if reader.fieldnames is None:
        raise DataQualityError(f"{source.source_type}: CSV header is missing")

    headers = [header.strip().lstrip("\ufeff") for header in reader.fieldnames]
    date_column = next((h for h in headers if h in DATE_COLUMN_CANDIDATES), None)
    if date_column is None:
        raise DataQualityError(
            f"{source.source_type}: expected date column in {DATE_COLUMN_CANDIDATES}, got {headers}"
        )

    indicator_columns = [name for name in headers if name != date_column]
    if not indicator_columns:
        return [], 0, 0

    measurements: dict[tuple[str, int, datetime, str], MeasurementRow] = {}
    parse_errors: list[str] = []
    rows_read = 0
    duplicates = 0

    for row in reader:
        rows_read += 1
        raw_dt = (row.get(date_column) or "").strip().strip('"')

        try:
            observed_at = datetime.strptime(raw_dt, "%Y-%m-%d %H:%M")
        except ValueError:
            parse_errors.append(f"invalid datetime '{raw_dt}' at row {rows_read}")
            continue

        for indicator_name in indicator_columns:
            raw_value = (row.get(indicator_name) or "").strip()
            indicator_code = normalize_indicator_code(indicator_name)

            try:
                value_numeric = parse_localized_numeric(raw_value)
            except ValueError:
                parse_errors.append(
                    f"invalid numeric '{raw_value}' for {indicator_name} at {observed_at.isoformat()}"
                )
                continue

            source_row_hash = hashlib.sha1(
                f"{source.source_type}|{source.station_id}|{observed_at.isoformat()}|{indicator_code}|{raw_value}".encode(
                    "utf-8"
                )
            ).hexdigest()

            record = MeasurementRow(
                source_type=source.source_type,
                station_id=source.station_id,
                observed_at=observed_at,
                indicator_code=indicator_code,
                indicator_name=indicator_name,
                value_numeric=value_numeric,
                source_row_hash=source_row_hash,
            )

            key = (
                record.source_type,
                record.station_id,
                record.observed_at,
                record.indicator_code,
            )
            if key in measurements:
                duplicates += 1
            measurements[key] = record

    if parse_errors:
        preview = "; ".join(parse_errors[:8])
        raise DataQualityError(f"{source.source_type}: {len(parse_errors)} parse errors. {preview}")

    return list(measurements.values()), rows_read, duplicates


def extract_window_with_split(
    settings: Settings,
    source: SourceConfig,
    window_start: date,
    window_end: date,
    summary: SourceRunSummary,
    progress: ProgressCallback | None = None,
) -> list[MeasurementRow]:
    """Extract and parse one window, recursively splitting on retriable failures."""

    summary.windows_requested += 1
    try:
        csv_text = fetch_source_window(
            settings=settings,
            source=source,
            window_start=window_start,
            window_end=window_end,
            retry_count=settings.request_retries,
            progress=progress,
        )
    except SourceFetchError as exc:
        span_days = (window_end - window_start).days + 1
        should_split = exc.retriable and span_days > settings.minimum_split_window_days
        if not should_split:
            raise

        summary.split_events += 1
        split_size = span_days // 2
        left_end = window_start + timedelta(days=split_size - 1)
        right_start = left_end + timedelta(days=1)
        if progress is not None:
            progress(
                {
                    "event": "window_split",
                    "source_key": source.source_key,
                    "source_type": source.source_type,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "left_window_start": window_start.isoformat(),
                    "left_window_end": left_end.isoformat(),
                    "right_window_start": right_start.isoformat(),
                    "right_window_end": window_end.isoformat(),
                    "split_events_total": summary.split_events,
                }
            )
        left = extract_window_with_split(
            settings,
            source,
            window_start,
            left_end,
            summary,
            progress=progress,
        )
        right = extract_window_with_split(
            settings,
            source,
            right_start,
            window_end,
            summary,
            progress=progress,
        )
        return left + right

    records, rows_read, duplicates = parse_airviro_csv(source, csv_text)
    summary.rows_read += rows_read
    summary.duplicate_measurements += duplicates
    return records


def build_source_records(
    settings: Settings,
    source: SourceConfig,
    start_date: date,
    end_date: date,
    summary: SourceRunSummary,
    progress: ProgressCallback | None = None,
) -> list[MeasurementRow]:
    """Extract all records for one source in the given range."""

    windows = date_chunks(start_date, end_date, source.max_window_days)
    total_windows = len(windows)
    if progress is not None:
        progress(
            {
                "event": "source_start",
                "source_key": source.source_key,
                "source_type": source.source_type,
                "source_station_id": source.station_id,
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "max_window_days": source.max_window_days,
                "top_level_window_count": total_windows,
            }
        )

    all_records: list[MeasurementRow] = []
    for index, (window_start, window_end) in enumerate(windows, start=1):
        if progress is not None:
            progress(
                {
                    "event": "top_level_window_start",
                    "source_key": source.source_key,
                    "source_type": source.source_type,
                    "window_index": index,
                    "window_count": total_windows,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                }
            )
        rows_before = summary.rows_read
        duplicates_before = summary.duplicate_measurements
        windows_requested_before = summary.windows_requested
        records = extract_window_with_split(
            settings=settings,
            source=source,
            window_start=window_start,
            window_end=window_end,
            summary=summary,
            progress=progress,
        )
        all_records.extend(records)
        if progress is not None:
            progress(
                {
                    "event": "top_level_window_complete",
                    "source_key": source.source_key,
                    "source_type": source.source_type,
                    "window_index": index,
                    "window_count": total_windows,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "rows_read_window": summary.rows_read - rows_before,
                    "rows_read_total": summary.rows_read,
                    "records_normalized_window": len(records),
                    "records_normalized_total": len(all_records),
                    "duplicates_window": summary.duplicate_measurements - duplicates_before,
                    "duplicates_total": summary.duplicate_measurements,
                    "windows_requested_window": summary.windows_requested - windows_requested_before,
                    "windows_requested_total": summary.windows_requested,
                    "split_events_total": summary.split_events,
                }
            )

    if progress is not None:
        progress(
            {
                "event": "source_complete",
                "source_key": source.source_key,
                "source_type": source.source_type,
                "top_level_window_count": total_windows,
                "windows_requested_total": summary.windows_requested,
                "rows_read_total": summary.rows_read,
                "records_normalized_total": len(all_records),
                "duplicates_total": summary.duplicate_measurements,
                "split_events_total": summary.split_events,
            }
        )
    return all_records


def summarize_indicator_counts(records: list[MeasurementRow]) -> dict[str, int]:
    """Count records per indicator code for quick diagnostics."""

    counter = Counter(record.indicator_code for record in records)
    return dict(sorted(counter.items(), key=lambda item: item[0]))
