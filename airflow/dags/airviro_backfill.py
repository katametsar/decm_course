"""Manual Airviro backfill DAG with configurable range/chunk parameters."""

from __future__ import annotations

from datetime import date
import os

from airflow.sdk import Param, dag, task
import pendulum

import airviro_dag_utils as utils


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, str(default)).strip().lower()
    return raw in {"1", "true", "yes", "on"}


@dag(
    dag_id="airviro_backfill",
    description="Manual backfill ETL + dbt pipeline for historical ranges.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2020-01-01", type="string"),
        "end_date": Param("", type="string"),
        "chunk_days": Param(31, type="integer", minimum=1),
        "advance_watermark": Param(True, type="boolean"),
    },
    tags=["course", "airviro", "etl", "dbt", "backfill"],
)
def airviro_backfill() -> None:
    @task(task_id="ensure_prerequisites")
    def ensure_prerequisites() -> None:
        utils.ensure_etl_schema()
        utils.ensure_watermark_table()

    @task(task_id="plan_backfill")
    def plan_backfill(
        start_date_raw: str,
        end_date_raw: str,
        chunk_days_raw: str,
        advance_watermark_raw: str,
    ) -> dict[str, object]:
        start_date = utils.parse_iso_date(start_date_raw)
        end_raw = str(end_date_raw).strip()
        end_date = utils.parse_iso_date(end_raw) if end_raw else utils.utc_today()
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")

        chunk_days = int(chunk_days_raw)
        if chunk_days < 1:
            raise ValueError("chunk_days must be >= 1")

        windows = [
            {"from_date": start.isoformat(), "to_date": end.isoformat()}
            for start, end in utils.split_date_range(start_date, end_date, chunk_days)
        ]

        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "chunk_days": chunk_days,
            "window_count": len(windows),
            "windows": windows,
            "advance_watermark": str(advance_watermark_raw).strip().lower()
            in {"1", "true", "yes", "on"},
        }

    @task(task_id="run_backfill_windows")
    def run_backfill_windows(plan: dict[str, object]) -> None:
        verbose = _env_bool("AIRFLOW_AIRVIRO_BACKFILL_VERBOSE", False)
        windows: list[dict[str, str]] = list(plan["windows"])  # type: ignore[arg-type]
        print(
            "[airviro] backfill plan: "
            f"{plan['start_date']}..{plan['end_date']} "
            f"in {plan['window_count']} windows (chunk_days={plan['chunk_days']})"
        )
        for index, window in enumerate(windows, start=1):
            from_date = utils.parse_iso_date(window["from_date"])
            to_date = utils.parse_iso_date(window["to_date"])
            print(
                "[airviro] backfill window "
                f"{index}/{len(windows)}: {from_date.isoformat()}..{to_date.isoformat()}"
            )
            utils.run_etl_range(from_date, to_date, verbose=verbose)

    @task(task_id="run_dbt_build")
    def run_dbt_build() -> None:
        utils.run_dbt_build()

    @task(task_id="maybe_advance_watermark")
    def maybe_advance_watermark(plan: dict[str, object]) -> None:
        if not bool(plan["advance_watermark"]):
            print("[airviro] skipping watermark update (advance_watermark=false)")
            return

        end_date = utils.parse_iso_date(str(plan["end_date"]))
        utils.set_watermark_greatest(utils.PIPELINE_NAME_INCREMENTAL, end_date)
        print(
            "[airviro] watermark updated with greatest(end_date): "
            f"{utils.PIPELINE_NAME_INCREMENTAL} -> {end_date.isoformat()}"
        )

    prerequisites = ensure_prerequisites()
    plan = plan_backfill(
        start_date_raw="{{ params.start_date }}",
        end_date_raw="{{ params.end_date }}",
        chunk_days_raw="{{ params.chunk_days }}",
        advance_watermark_raw="{{ params.advance_watermark }}",
    )
    backfill = run_backfill_windows(plan)
    dbt_task = run_dbt_build()
    watermark = maybe_advance_watermark(plan)

    prerequisites >> plan >> backfill >> dbt_task >> watermark


airviro_backfill()
