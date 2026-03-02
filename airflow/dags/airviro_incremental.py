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

    @task(task_id="plan_incremental_window")
    def plan_incremental_window() -> dict[str, object]:
        bootstrap_start = utils.parse_iso_date(
            os.getenv("AIRFLOW_AIRVIRO_INCREMENTAL_BOOTSTRAP_START", "2020-01-01")
        )
        max_days_per_run = int(os.getenv("AIRFLOW_AIRVIRO_INCREMENTAL_MAX_DAYS", "31"))
        if max_days_per_run < 1:
            raise ValueError("AIRFLOW_AIRVIRO_INCREMENTAL_MAX_DAYS must be >= 1")

        pipeline_name = utils.PIPELINE_NAME_INCREMENTAL
        watermark = utils.get_watermark(pipeline_name)
        today = utils.utc_today()

        if watermark is None:
            start_date = bootstrap_start
            watermark_source = "bootstrap_start"
        else:
            start_date = watermark + timedelta(days=1)
            watermark_source = "stored_watermark"

        # If watermark was advanced to today (legacy behavior), clamp to today's
        # window so hourly runs can continue refreshing current-day data.
        if watermark is not None and start_date > today:
            start_date = today
            watermark_source = f"{watermark_source}_clamped_today"

        if start_date > today:
            return {
                "has_work": False,
                "pipeline_name": pipeline_name,
                "watermark_source": watermark_source,
                "watermark_date": watermark.isoformat() if watermark else None,
                "from_date": start_date.isoformat(),
                "to_date": today.isoformat(),
            }

        end_date = min(today, start_date + timedelta(days=max_days_per_run - 1))
        closed_day = today - timedelta(days=1)
        # Watermark tracks only fully closed dates; current date is reloaded hourly.
        watermark_target = min(end_date, closed_day)
        return {
            "has_work": True,
            "pipeline_name": pipeline_name,
            "watermark_source": watermark_source,
            "watermark_date": watermark.isoformat() if watermark else None,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "watermark_target_date": watermark_target.isoformat(),
        }

    @task.branch(task_id="choose_path")
    def choose_path(plan: dict[str, object]) -> str:
        if bool(plan["has_work"]):
            return "run_etl_window"
        return "no_work"

    @task(task_id="run_etl_window")
    def run_etl_window(plan: dict[str, object]) -> None:
        start_date = utils.parse_iso_date(str(plan["from_date"]))
        end_date = utils.parse_iso_date(str(plan["to_date"]))
        verbose = _env_bool("AIRFLOW_AIRVIRO_INCREMENTAL_VERBOSE", False)
        utils.run_etl_range(start_date, end_date, verbose=verbose)

    @task(task_id="run_dbt_build")
    def run_dbt_build() -> None:
        utils.run_dbt_build()

    @task(task_id="advance_watermark")
    def advance_watermark(plan: dict[str, object]) -> None:
        pipeline_name = str(plan["pipeline_name"])
        watermark_target_date = utils.parse_iso_date(str(plan["watermark_target_date"]))
        utils.set_watermark(pipeline_name, watermark_target_date)
        print(
            f"[airviro] advanced watermark '{pipeline_name}' "
            f"to {watermark_target_date.isoformat()}"
        )

    @task(task_id="no_work")
    def no_work(plan: dict[str, object]) -> None:
        print(
            "[airviro] no incremental work window "
            f"(from={plan['from_date']}, to={plan['to_date']}, source={plan['watermark_source']})"
        )

    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    prerequisites = ensure_prerequisites()
    plan = plan_incremental_window()
    branch = choose_path(plan)

    etl_task = run_etl_window(plan)
    dbt_task = run_dbt_build()
    watermark_task = advance_watermark(plan)
    no_work_task = no_work(plan)

    prerequisites >> plan >> branch
    branch >> etl_task >> dbt_task >> watermark_task >> done
    branch >> no_work_task >> done


airviro_incremental()
