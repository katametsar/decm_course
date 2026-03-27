"""Extraction and transformation logic for the Ohuseire monitoring API."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time as day_time, timedelta
import hashlib
import html
import json
import re
import time
from typing import Callable
import unicodedata
from urllib import error, parse, request

from .config import Settings


API_TYPE_BY_SOURCE_TYPE = {
    "air_quality": "INDICATOR",
    "pollen": "POLLEN",
}
KNOWN_INDICATOR_CODE_ALIASES = {
    "temp10": "temp",
    "temp_10": "temp",
    "temperature_at_10_m": "temp",
}
# The live station-8 air-quality response for 2025-10-26 mixes DST duplicate-hour
# behavior with staggered historical timestamps. Skip that date rather than guess.
UNSAFE_SOURCE_DATES_BY_SOURCE_KEY = {
    "air_quality_station_8": (
        date(2025, 10, 26),
    ),
}
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
class IndicatorMetadata:
    """Source indicator metadata discovered from the API."""

    indicator_id: int
    indicator_code: str
    indicator_name: str


@dataclass(frozen=True)
class StationMetadata:
    """Source station metadata discovered from the API."""

    station_id: int
    station_name: str
    station_type: str
    airviro_code: str
    indicator_ids: tuple[int, ...]


@dataclass(frozen=True)
class SourceConfig:
    """Extraction configuration for one lecture source."""

    source_key: str
    source_type: str
    station_id: int
    station_name: str
    station_airviro_code: str
    api_type: str
    ordered_indicator_ids: tuple[int, ...]
    indicator_metadata_by_id: dict[int, IndicatorMetadata]
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
    warnings: list[str] = field(default_factory=list)


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


@dataclass(frozen=True)
class MonitoringApiRow:
    """One measurement row returned by the monitoring endpoint."""

    measured_at: datetime
    indicator_id: int
    raw_value: str
    value_numeric: float | None


def parse_iso_date(raw: str) -> date:
    """Parse yyyy-mm-dd date strings."""

    return datetime.strptime(raw, "%Y-%m-%d").date()


def format_airviro_date(value: date) -> str:
    """Format dates for Ohuseire query parameters (dd.mm.yyyy)."""

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


def split_date_range_excluding_dates(
    start_date: date,
    end_date: date,
    excluded_dates: tuple[date, ...],
) -> list[tuple[date, date]]:
    """Split an inclusive range into contiguous spans that omit excluded dates."""

    skipped_dates = sorted(
        {
            excluded_date
            for excluded_date in excluded_dates
            if start_date <= excluded_date <= end_date
        }
    )
    if not skipped_dates:
        return [(start_date, end_date)]

    spans: list[tuple[date, date]] = []
    current_start = start_date
    for skipped_date in skipped_dates:
        span_end = skipped_date - timedelta(days=1)
        if current_start <= span_end:
            spans.append((current_start, span_end))
        current_start = skipped_date + timedelta(days=1)

    if current_start <= end_date:
        spans.append((current_start, end_date))

    return spans


def build_guarded_source_windows(
    source: SourceConfig,
    start_date: date,
    end_date: date,
) -> tuple[list[tuple[date, date]], list[str]]:
    """Build extraction windows while skipping source dates known to be unsafe."""

    unsafe_dates = tuple(
        skipped_date
        for skipped_date in UNSAFE_SOURCE_DATES_BY_SOURCE_KEY.get(source.source_key, ())
        if start_date <= skipped_date <= end_date
    )
    if not unsafe_dates:
        return date_chunks(start_date, end_date, source.max_window_days), []

    safe_spans = split_date_range_excluding_dates(start_date, end_date, unsafe_dates)
    windows: list[tuple[date, date]] = []
    for span_start, span_end in safe_spans:
        windows.extend(date_chunks(span_start, span_end, source.max_window_days))

    skipped_days = ", ".join(item.isoformat() for item in unsafe_dates)
    if windows:
        warning = (
            f"{source.source_key}: skipped unsafe source date(s) {skipped_days} during "
            "window selection because the live API mixes DST duplicate-hour timestamps "
            "with staggered historical timestamps on that day"
        )
    else:
        warning = (
            f"{source.source_key}: requested window {start_date.isoformat()}.."
            f"{end_date.isoformat()} contains only unsafe source date(s) {skipped_days}; "
            "no source windows were extracted"
        )
    return windows, [warning]


def build_api_url(
    settings: Settings,
    endpoint: str,
    params: dict[str, str] | None = None,
) -> str:
    """Build one Ohuseire API URL from endpoint path and query parameters."""

    base_url = settings.airviro_base_url.rstrip("/")
    url = f"{base_url}/{endpoint.lstrip('/')}"
    if params:
        url = f"{url}?{parse.urlencode(params)}"
    return url


def strip_html_tags(raw_value: str | None) -> str:
    """Convert HTML-rich API fields into readable plain text."""

    if not raw_value:
        return ""

    without_tags = re.sub(r"<[^>]*>", "", raw_value)
    cleaned = without_tags.replace("<", "").replace(">", "")
    return html.unescape(cleaned).strip()


def normalize_indicator_name(raw_name: str) -> str:
    """Create stable ascii indicator codes from API metadata."""

    normalized = unicodedata.normalize("NFKD", raw_name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_name = ascii_name.replace("%", "pct").replace(".", "_")
    ascii_name = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_name).strip("_").lower()
    if not ascii_name:
        return "unknown_indicator"
    if ascii_name[0].isdigit():
        return f"i_{ascii_name}"
    return KNOWN_INDICATOR_CODE_ALIASES.get(ascii_name, ascii_name)


def build_indicator_metadata(raw_item: dict[str, object]) -> IndicatorMetadata:
    """Map API indicator metadata to stable lecture indicator fields."""

    indicator_id = int(raw_item["id"])
    formula_text = strip_html_tags(str(raw_item.get("formula") or ""))
    name_text = strip_html_tags(str(raw_item.get("name") or ""))
    code_seed = formula_text or name_text or f"indicator_{indicator_id}"
    indicator_code = normalize_indicator_name(code_seed)
    indicator_name = name_text or formula_text or code_seed
    return IndicatorMetadata(
        indicator_id=indicator_id,
        indicator_code=indicator_code,
        indicator_name=indicator_name,
    )


def fetch_json_payload(settings: Settings, url: str) -> object:
    """Fetch one JSON payload from the live Ohuseire API."""

    last_error: Exception | None = None
    for attempt in range(1, settings.request_retries + 1):
        try:
            with request.urlopen(url, timeout=settings.request_timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8-sig"))
        except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == settings.request_retries:
                break
            time.sleep(2**attempt)

    raise PipelineError(f"Failed to fetch API metadata from {url}: {last_error}")


def fetch_station_catalog(settings: Settings) -> dict[int, StationMetadata]:
    """Fetch station metadata keyed by station id."""

    url = build_api_url(settings, f"station/{settings.airviro_locale}")
    payload = fetch_json_payload(settings, url)
    if not isinstance(payload, dict) or not isinstance(payload.get("features"), list):
        raise PipelineError("Station API returned an unexpected payload shape")

    catalog: dict[int, StationMetadata] = {}
    for feature in payload["features"]:
        if not isinstance(feature, dict):
            continue

        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue

        try:
            station_id = int(feature["id"])
        except (KeyError, TypeError, ValueError):
            continue

        indicator_ids_raw = properties.get("indicators") or []
        if not isinstance(indicator_ids_raw, list):
            indicator_ids_raw = []

        indicator_ids = tuple(
            int(value)
            for value in indicator_ids_raw
            if isinstance(value, (int, float, str)) and str(value).strip()
        )
        catalog[station_id] = StationMetadata(
            station_id=station_id,
            station_name=str(properties.get("name") or f"station_{station_id}").strip(),
            station_type=str(properties.get("type") or "").strip(),
            airviro_code=str(properties.get("airviro_code") or "").strip(),
            indicator_ids=indicator_ids,
        )

    return catalog


def fetch_indicator_catalog(
    settings: Settings,
    api_type: str,
) -> dict[int, IndicatorMetadata]:
    """Fetch indicator metadata for one API type."""

    url = build_api_url(
        settings,
        f"indicator/{settings.airviro_locale}",
        params={"type": api_type},
    )
    payload = fetch_json_payload(settings, url)
    if not isinstance(payload, list):
        raise PipelineError(f"Indicator API returned unexpected payload for type={api_type}")

    catalog: dict[int, IndicatorMetadata] = {}
    for item in payload:
        if not isinstance(item, dict) or "id" not in item:
            continue
        metadata = build_indicator_metadata(item)
        catalog[metadata.indicator_id] = metadata
    return catalog


def get_source_configs(
    settings: Settings,
    requested_source_keys: set[str] | None = None,
) -> list[SourceConfig]:
    """Return source definitions discovered from the live API."""

    sources: list[SourceConfig] = []
    station_catalog = fetch_station_catalog(settings)
    indicator_catalogs = {
        api_type: fetch_indicator_catalog(settings, api_type)
        for api_type in sorted(set(API_TYPE_BY_SOURCE_TYPE.values()))
    }

    for station_id in settings.air_station_ids:
        source_key = f"air_quality_station_{station_id}"
        if requested_source_keys is not None and source_key not in requested_source_keys:
            continue
        station = station_catalog.get(station_id)
        if station is None:
            raise PipelineError(f"Air-quality station {station_id} was not found in station API")
        sources.append(
            SourceConfig(
                source_key=source_key,
                source_type="air_quality",
                station_id=station_id,
                station_name=station.station_name,
                station_airviro_code=station.airviro_code,
                api_type=API_TYPE_BY_SOURCE_TYPE["air_quality"],
                ordered_indicator_ids=station.indicator_ids,
                indicator_metadata_by_id=indicator_catalogs["INDICATOR"],
                max_window_days=settings.air_quality_window_days,
                extra_params={
                    "type": API_TYPE_BY_SOURCE_TYPE["air_quality"],
                    "indicators": ",".join(str(value) for value in station.indicator_ids),
                },
            )
        )

    for station_id in settings.pollen_station_ids:
        source_key = f"pollen_station_{station_id}"
        if requested_source_keys is not None and source_key not in requested_source_keys:
            continue
        station = station_catalog.get(station_id)
        if station is None:
            raise PipelineError(f"Pollen station {station_id} was not found in station API")
        sources.append(
            SourceConfig(
                source_key=source_key,
                source_type="pollen",
                station_id=station_id,
                station_name=station.station_name,
                station_airviro_code=station.airviro_code,
                api_type=API_TYPE_BY_SOURCE_TYPE["pollen"],
                ordered_indicator_ids=station.indicator_ids,
                indicator_metadata_by_id=indicator_catalogs["POLLEN"],
                max_window_days=settings.pollen_window_days,
                extra_params={
                    "type": API_TYPE_BY_SOURCE_TYPE["pollen"],
                    "indicators": ",".join(str(value) for value in station.indicator_ids),
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
    """Fetch one raw monitoring JSON response for a date window."""

    params: dict[str, str] = {
        "stations": str(source.station_id),
        "range": ",".join(
            [format_airviro_date(window_start), format_airviro_date(window_end)]
        ),
    }
    params.update(source.extra_params)

    url = build_api_url(settings, f"monitoring/{settings.airviro_locale}", params)

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


def parse_localized_numeric(raw_value: str) -> float | None:
    """Parse JSON/string numeric text and return numeric or null."""

    value = raw_value.strip().strip('"')
    if value in {"", "-", "NA", "N/A", "null", "NULL"}:
        return None

    compact = value.replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    compact = compact.replace(",", ".")
    return float(compact)


def should_normalize_staggered_rows(
    source: SourceConfig,
    rows: list[MonitoringApiRow],
    window_start: date,
    window_end: date,
) -> bool:
    """Detect older API payloads whose timestamps are staggered by indicator order."""

    if not rows or not source.ordered_indicator_ids:
        return False

    expected_slot_count = (window_end - window_start).days + 1
    if source.source_type != "pollen":
        expected_slot_count *= 24

    distinct_timestamps = {row.measured_at for row in rows}
    return len(distinct_timestamps) > expected_slot_count


def normalize_staggered_rows(
    source: SourceConfig,
    rows: list[MonitoringApiRow],
) -> list[MonitoringApiRow]:
    """Shift older staggered timestamps back to the source grain."""

    indicator_offsets = {
        indicator_id: index + 1
        for index, indicator_id in enumerate(source.ordered_indicator_ids)
    }

    normalized: list[MonitoringApiRow] = []
    for row in rows:
        offset_hours = indicator_offsets.get(row.indicator_id)
        if offset_hours is None:
            return rows
        normalized.append(
            MonitoringApiRow(
                measured_at=row.measured_at - timedelta(hours=offset_hours),
                indicator_id=row.indicator_id,
                raw_value=row.raw_value,
                value_numeric=row.value_numeric,
            )
        )
    return normalized


def parse_monitoring_json(
    source: SourceConfig,
    payload_text: str,
    window_start: date,
    window_end: date,
) -> tuple[list[MeasurementRow], int, int]:
    """Parse monitoring JSON into normalized long-form measurements."""

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise DataQualityError(f"{source.source_type}: response was not valid JSON") from exc

    if not isinstance(payload, list):
        raise DataQualityError(f"{source.source_type}: expected JSON list response")

    parse_errors: list[str] = []
    api_rows: list[MonitoringApiRow] = []
    rows_read = 0

    for item in payload:
        rows_read += 1
        if not isinstance(item, dict):
            parse_errors.append(f"row {rows_read} is not an object")
            continue

        raw_dt = str(item.get("measured") or "").strip()
        raw_value = "" if item.get("value") is None else str(item.get("value")).strip()

        try:
            station_id = int(item.get("station"))
        except (TypeError, ValueError):
            parse_errors.append(f"row {rows_read} has invalid station id")
            continue
        if station_id != source.station_id:
            parse_errors.append(
                f"row {rows_read} returned station_id={station_id}, expected {source.station_id}"
            )
            continue

        try:
            measured_at = datetime.strptime(raw_dt, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            parse_errors.append(f"invalid datetime '{raw_dt}' at row {rows_read}")
            continue

        try:
            indicator_id = int(item.get("indicator"))
        except (TypeError, ValueError):
            parse_errors.append(f"row {rows_read} has invalid indicator id")
            continue

        try:
            value_numeric = parse_localized_numeric(raw_value)
        except ValueError:
            parse_errors.append(
                f"invalid numeric '{raw_value}' for indicator_id={indicator_id} at {raw_dt}"
            )
            continue

        api_rows.append(
            MonitoringApiRow(
                measured_at=measured_at,
                indicator_id=indicator_id,
                raw_value=raw_value,
                value_numeric=value_numeric,
            )
        )

    if parse_errors:
        preview = "; ".join(parse_errors[:8])
        raise DataQualityError(f"{source.source_type}: {len(parse_errors)} parse errors. {preview}")

    if should_normalize_staggered_rows(source, api_rows, window_start, window_end):
        api_rows = normalize_staggered_rows(source, api_rows)

    measurements: dict[tuple[str, int, datetime, str], MeasurementRow] = {}
    duplicates = 0

    for row in api_rows:
        metadata = source.indicator_metadata_by_id.get(row.indicator_id)
        if metadata is None:
            raise DataQualityError(
                f"{source.source_type}: indicator_id={row.indicator_id} was not found in API metadata"
            )

        source_row_hash = hashlib.sha1(
            f"{source.source_type}|{source.station_id}|{row.measured_at.isoformat()}|{metadata.indicator_code}|{row.raw_value}".encode(
                "utf-8"
            )
        ).hexdigest()

        record = MeasurementRow(
            source_type=source.source_type,
            station_id=source.station_id,
            observed_at=row.measured_at,
            indicator_code=metadata.indicator_code,
            indicator_name=metadata.indicator_name,
            value_numeric=row.value_numeric,
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
        payload_text = fetch_source_window(
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

    records, rows_read, duplicates = parse_monitoring_json(
        source,
        payload_text,
        window_start,
        window_end,
    )
    summary.rows_read += rows_read
    summary.duplicate_measurements += duplicates
    coverage_warning = build_window_coverage_warning(
        source=source,
        window_start=window_start,
        window_end=window_end,
        records=records,
    )
    if coverage_warning is not None:
        summary.warnings.append(coverage_warning)
        if progress is not None:
            progress(
                {
                    "event": "coverage_warning",
                    "source_key": source.source_key,
                    "source_type": source.source_type,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "warning": coverage_warning,
                }
            )
    return records


def build_window_coverage_warning(
    *,
    source: SourceConfig,
    window_start: date,
    window_end: date,
    records: list[MeasurementRow],
) -> str | None:
    """Return a non-fatal warning when response timestamps do not reach window bounds."""

    if not records:
        return (
            f"{source.source_key}: no timestamps returned for requested window "
            f"{window_start.isoformat()}..{window_end.isoformat()}"
        )

    first_observed_at = min(record.observed_at for record in records)
    last_observed_at = max(record.observed_at for record in records)

    expected_start = datetime.combine(window_start, day_time.min)
    if source.source_type == "pollen":
        # Pollen observations are treated as daily for completeness reporting.
        missing_start = first_observed_at.date() > window_start
        missing_end = last_observed_at.date() < window_end
    else:
        expected_end = datetime.combine(window_end, day_time(hour=23))
        missing_start = first_observed_at > expected_start
        missing_end = last_observed_at < expected_end

    if not missing_start and not missing_end:
        return None

    missing_parts: list[str] = []
    if missing_start:
        missing_parts.append("start")
    if missing_end:
        missing_parts.append("end")

    return (
        f"{source.source_key}: returned timestamps do not fully cover requested window "
        f"{window_start.isoformat()}..{window_end.isoformat()} "
        f"(missing {', '.join(missing_parts)}; "
        f"first_observed_at={first_observed_at.isoformat(sep=' ', timespec='minutes')}, "
        f"last_observed_at={last_observed_at.isoformat(sep=' ', timespec='minutes')})"
    )


def build_source_records(
    settings: Settings,
    source: SourceConfig,
    start_date: date,
    end_date: date,
    summary: SourceRunSummary,
    progress: ProgressCallback | None = None,
) -> list[MeasurementRow]:
    """Extract all records for one source in the given range."""

    windows, window_guard_warnings = build_guarded_source_windows(
        source,
        start_date,
        end_date,
    )
    total_windows = len(windows)
    for warning in window_guard_warnings:
        summary.warnings.append(warning)
        if progress is not None:
            progress(
                {
                    "event": "window_guard",
                    "source_key": source.source_key,
                    "source_type": source.source_type,
                    "warning": warning,
                }
            )
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

    all_records: dict[tuple[str, int, datetime, str], MeasurementRow] = {}
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
        cross_window_duplicates = 0
        for record in records:
            key = (
                record.source_type,
                record.station_id,
                record.observed_at,
                record.indicator_code,
            )
            if key in all_records:
                cross_window_duplicates += 1
            all_records[key] = record
        summary.duplicate_measurements += cross_window_duplicates
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
                    "records_normalized_window": len(records) - cross_window_duplicates,
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
    return list(all_records.values())


def summarize_indicator_counts(records: list[MeasurementRow]) -> dict[str, int]:
    """Count records per indicator code for quick diagnostics."""

    counter = Counter(record.indicator_code for record in records)
    return dict(sorted(counter.items(), key=lambda item: item[0]))
