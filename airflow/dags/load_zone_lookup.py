"""Load the TLC taxi zone lookup CSV into raw.taxi_zones.

Run on demand (no schedule). Each run replaces the table's contents.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from ingestion import loaders

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/project/data")) / "raw"


@dag(
    dag_id="load_zone_lookup",
    description="Download the TLC taxi zone lookup and replace raw.taxi_zones",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["ingestion", "nyc-taxi"],
)
def load_zone_lookup():
    @task
    def download() -> str:
        dest = DATA_DIR / "taxi_zone_lookup.csv"
        try:
            loaders.download_file(loaders.ZONE_LOOKUP_URL, dest)
        except loaders.FileNotPublishedError as exc:
            raise AirflowFailException(
                f"{exc} Download the CSV from the TLC page and update ZONE_LOOKUP_URL."
            ) from exc
        return str(dest)

    @task
    def validate(path: str) -> str:
        try:
            loaders.validate_zone_file(path)
        except loaders.ValidationError as exc:
            raise AirflowFailException(str(exc)) from exc
        return path

    @task
    def load(path: str) -> int:
        with loaders.connect() as conn:
            return loaders.load_zone_file(conn, path)

    load(validate(download()))


load_zone_lookup()
