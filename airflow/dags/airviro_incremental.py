"""Incremental Airviro pipeline DAG with watermark-based progress tracking."""

from __future__ import annotations

from datetime import timedelta
import os

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
import pendulum

import airviro_dag_utils as utils


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, str(default)).strip().lower()
    return raw in {"1", "true", "yes", "on"}


@dag(
    dag_id="airviro_incremental",
    description="Incremental ETL + dbt orchestration with date watermark state.",
    schedule="15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["course", "airviro", "etl", "dbt", "incremental"],
)
def airviro_incremental() -> None:
    @task(task_id="ensure_prerequisites")
    def ensure_prerequisites() -> None:
        # Keep this idempotent so students can recover by re-running the DAG.
        utils.ensure_etl_schema()
        utils.ensure_watermark_table()

    @task(task_id="plan_incremental_windows")
    def plan_incremental_windows() -> dict[str, object]:
        bootstrap_start = utils.parse_iso_date(
            os.getenv("AIRFLOW_AIRVIRO_INCREMENTAL_BOOTSTRAP_START", "2020-01-01")
        )
        max_days_per_run = int(os.getenv("AIRFLOW_AIRVIRO_INCREMENTAL_MAX_DAYS", "31"))
        if max_days_per_run < 1:
            raise ValueError("AIRFLOW_AIRVIRO_INCREMENTAL_MAX_DAYS must be >= 1")

        today = utils.utc_today()
        closed_day = today - timedelta(days=1)
        legacy_global_watermark = utils.get_watermark(utils.PIPELINE_NAME_INCREMENTAL)

        source_windows: list[dict[str, object]] = []
        for source in utils.get_configured_sources():
            source_key = str(source["source_key"])
            watermark_key = utils.incremental_source_watermark_key(source_key)
            watermark = utils.get_watermark(watermark_key)

            if watermark is None:
                if legacy_global_watermark is not None:
                    start_date = legacy_global_watermark + timedelta(days=1)
                    watermark_source = "legacy_global_watermark"
                else:
                    start_date = bootstrap_start
                    watermark_source = "bootstrap_start"
            else:
                anchor_watermark = watermark
                watermark_source = "stored_watermark"
                if (
                    legacy_global_watermark is not None
                    and anchor_watermark < legacy_global_watermark
                ):
                    anchor_watermark = legacy_global_watermark
                    watermark_source = "stored_watermark_promoted_from_legacy"
                start_date = anchor_watermark + timedelta(days=1)

            # If watermark was advanced to today (legacy behavior), clamp to today's
            # window so hourly runs can continue refreshing current-day data.
            if watermark is not None and start_date > today:
                start_date = today
                watermark_source = f"{watermark_source}_clamped_today"

            if start_date > today:
                source_windows.append(
                    {
                        "source_key": source_key,
                        "source_type": str(source["source_type"]),
                        "station_id": int(source["station_id"]),
                        "watermark_key": watermark_key,
                        "watermark_source": watermark_source,
                        "watermark_date": watermark.isoformat() if watermark else None,
                        "from_date": start_date.isoformat(),
                        "to_date": today.isoformat(),
                        "has_work": False,
                    }
                )
                continue

            end_date = min(today, start_date + timedelta(days=max_days_per_run - 1))
            # Watermark tracks only fully closed dates; current date is reloaded hourly.
            watermark_target = min(end_date, closed_day)
            source_windows.append(
                {
                    "source_key": source_key,
                    "source_type": str(source["source_type"]),
                    "station_id": int(source["station_id"]),
                    "watermark_key": watermark_key,
                    "watermark_source": watermark_source,
                    "watermark_date": watermark.isoformat() if watermark else None,
                    "from_date": start_date.isoformat(),
                    "to_date": end_date.isoformat(),
                    "watermark_target_date": watermark_target.isoformat(),
                    "has_work": True,
                }
            )

        work_window_count = sum(1 for item in source_windows if bool(item["has_work"]))
        return {
            "has_work": work_window_count > 0,
            "source_window_count": len(source_windows),
            "work_window_count": work_window_count,
            "source_windows": source_windows,
        }

    @task.branch(task_id="choose_path")
    def choose_path(plan: dict[str, object]) -> str:
        if bool(plan["has_work"]):
            return "run_etl_windows"
        return "no_work"

    @task(task_id="run_etl_windows")
    def run_etl_windows(plan: dict[str, object]) -> None:
        verbose = _env_bool("AIRFLOW_AIRVIRO_INCREMENTAL_VERBOSE", False)
        source_windows: list[dict[str, object]] = list(plan["source_windows"])  # type: ignore[arg-type]
        for window in source_windows:
            if not bool(window["has_work"]):
                continue
            source_key = str(window["source_key"])
            start_date = utils.parse_iso_date(str(window["from_date"]))
            end_date = utils.parse_iso_date(str(window["to_date"]))
            print(
                f"[airviro] incremental source window "
                f"{source_key}: {start_date.isoformat()}..{end_date.isoformat()}"
            )
            utils.run_etl_range(start_date, end_date, verbose=verbose, source_key=source_key)

    @task(task_id="run_dbt_build")
    def run_dbt_build() -> None:
        utils.run_dbt_build()

    @task(task_id="advance_watermark")
    def advance_watermark(plan: dict[str, object]) -> None:
        source_windows: list[dict[str, object]] = list(plan["source_windows"])  # type: ignore[arg-type]
        for window in source_windows:
            if not bool(window["has_work"]):
                continue
            watermark_key = str(window["watermark_key"])
            watermark_target_date = utils.parse_iso_date(str(window["watermark_target_date"]))
            utils.set_watermark(watermark_key, watermark_target_date)
            print(
                f"[airviro] advanced watermark '{watermark_key}' "
                f"to {watermark_target_date.isoformat()}"
            )

    @task(task_id="no_work")
    def no_work(plan: dict[str, object]) -> None:
        print(
            "[airviro] no incremental work window "
            f"(source_window_count={plan['source_window_count']}, "
            f"work_window_count={plan['work_window_count']})"
        )

    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    prerequisites = ensure_prerequisites()
    plan = plan_incremental_windows()
    branch = choose_path(plan)

    etl_task = run_etl_windows(plan)
    dbt_task = run_dbt_build()
    watermark_task = advance_watermark(plan)
    no_work_task = no_work(plan)

    prerequisites >> plan >> branch
    branch >> etl_task >> dbt_task >> watermark_task >> done
    branch >> no_work_task >> done


airviro_incremental()
