"""Configuration and environment helpers for the Airviro ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import socket
import subprocess


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE pairs from an .env file if present.

    Existing environment variables are kept as-is to allow runtime overrides.
    """

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip()


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw)


def _as_int_tuple(name: str, default: tuple[int, ...]) -> tuple[int, ...]:
    """Parse comma-separated integer env values into a stable tuple."""

    raw = os.getenv(name, "").strip()
    if not raw:
        return default

    values: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        value = int(item)
        if value in seen:
            continue
        seen.add(value)
        values.append(value)

    if not values:
        return default
    return tuple(values)


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for extraction and loading."""

    airviro_base_url: str
    air_station_ids: tuple[int, ...]
    pollen_station_ids: tuple[int, ...]
    request_timeout_seconds: int
    request_retries: int
    minimum_split_window_days: int
    air_quality_window_days: int
    pollen_window_days: int
    warehouse_db_name: str
    warehouse_db_user: str
    warehouse_db_password: str
    warehouse_db_port: int
    warehouse_db_host: str

    @classmethod
    def from_env(cls) -> "Settings":
        air_station_ids = _as_int_tuple("AIRVIRO_AIR_STATION_IDS", ())
        if not air_station_ids:
            air_station_ids = (_as_int("AIRVIRO_AIR_STATION_ID", 8),)

        pollen_station_ids = _as_int_tuple("AIRVIRO_POLLEN_STATION_IDS", ())
        if not pollen_station_ids:
            pollen_station_ids = (_as_int("AIRVIRO_POLLEN_STATION_ID", 25),)

        return cls(
            airviro_base_url=os.getenv(
                "AIRVIRO_BASE_URL", "https://airviro.klab.ee/station/csv"
            ).strip(),
            air_station_ids=air_station_ids,
            pollen_station_ids=pollen_station_ids,
            request_timeout_seconds=_as_int("AIRVIRO_TIMEOUT_SECONDS", 45),
            request_retries=_as_int("AIRVIRO_REQUEST_RETRIES", 3),
            minimum_split_window_days=_as_int("AIRVIRO_MIN_SPLIT_DAYS", 7),
            air_quality_window_days=_as_int("AIRVIRO_AIR_WINDOW_DAYS", 300),
            pollen_window_days=_as_int("AIRVIRO_POLLEN_WINDOW_DAYS", 365),
            warehouse_db_name=os.getenv("WAREHOUSE_DB_NAME", "warehouse").strip(),
            warehouse_db_user=os.getenv("WAREHOUSE_DB_USER", "warehouse").strip(),
            warehouse_db_password=os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse").strip(),
            warehouse_db_port=_as_int("WAREHOUSE_DB_PORT", 5432),
            warehouse_db_host=os.getenv("WAREHOUSE_DB_HOST", "postgres").strip(),
        )

    @property
    def air_station_id(self) -> int:
        """Backward-compatible first air station id accessor."""

        return self.air_station_ids[0]

    @property
    def pollen_station_id(self) -> int:
        """Backward-compatible first pollen station id accessor."""

        return self.pollen_station_ids[0]

    def candidate_db_hosts(self) -> list[str]:
        """Candidate hostnames to support both devcontainer and compose contexts."""

        candidates: list[str] = []

        for host in self.warehouse_db_host.split(","):
            normalized = host.strip()
            if normalized and normalized not in candidates:
                candidates.append(normalized)

        gateway = _default_gateway_ip()
        fallback_hosts = [
            "postgres",
            "host.docker.internal",
            "localhost",
            "127.0.0.1",
        ]
        if gateway:
            fallback_hosts.insert(2, gateway)

        for fallback in fallback_hosts:
            if fallback not in candidates:
                candidates.append(fallback)

        # If the devcontainer is attached to compose network, use service DNS first.
        candidates = _promote_if_resolves(candidates, "postgres")

        return candidates


def _default_gateway_ip() -> str | None:
    """Read default gateway IP from container routing table if available."""

    try:
        output = subprocess.check_output(
            ["ip", "-4", "route", "show", "default"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:  # pragma: no cover - environment dependent
        return None

    parts = output.split()
    if "via" not in parts:
        return None
    via_index = parts.index("via")
    if via_index + 1 >= len(parts):
        return None
    return parts[via_index + 1].strip()


def _promote_if_resolves(hosts: list[str], preferred_host: str) -> list[str]:
    """Move host to the front if DNS resolution works in current runtime."""

    try:
        socket.getaddrinfo(preferred_host, None)
    except OSError:
        return hosts

    return [preferred_host] + [host for host in hosts if host != preferred_host]
