"""Monthly ingestion of NYC yellow taxi trips into raw.yellow_trips.

download -> validate -> load

Scheduled runs load the month PUBLICATION_LAG_MONTHS before the run date,
because TLC publishes each month with a delay. Trigger manually with
`year_month` (YYYY-MM) to load a specific month; re-running a month replaces
its rows, so it never creates duplicates.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from ingestion import loaders

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/project/data")) / "raw"
PUBLICATION_LAG_MONTHS = 3


@dag(
    dag_id="ingest_yellow_trips",
    description="Download a month of TLC yellow taxi trips and load it into raw.yellow_trips",
    schedule="0 6 1 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    params={
        "year_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description=(
                "Month to load as YYYY-MM. Leave empty to load the month "
                f"{PUBLICATION_LAG_MONTHS} months before the run date."
            ),
        ),
    },
    tags=["ingestion", "nyc-taxi"],
)
def ingest_yellow_trips():
    @task
    def resolve_month() -> str:
        context = get_current_context()
        year_month = context["params"].get("year_month")
        if year_month:
            return year_month
        run_date = context.get("logical_date") or context["dag_run"].run_after
        return loaders.shift_month(run_date.strftime("%Y-%m"), -PUBLICATION_LAG_MONTHS)

    @task
    def download(year_month: str) -> str:
        dest = DATA_DIR / loaders.trip_file_name(year_month)
        try:
            loaders.download_file(loaders.trip_file_url(year_month), dest)
        except loaders.FileNotPublishedError as exc:
            # Retrying will not help until TLC publishes the file.
            raise AirflowFailException(str(exc)) from exc
        return str(dest)

    @task
    def validate(path: str) -> int:
        try:
            return loaders.validate_trip_file(path)
        except loaders.ValidationError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def load(path: str, expected_rows: int) -> int:
        with loaders.connect() as conn:
            return loaders.load_trip_file(conn, path, expected_rows)

    path = download(resolve_month())
    load(path, validate(path))


ingest_yellow_trips()
