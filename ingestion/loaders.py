"""Shared download and load functions for the NYC TLC raw data.

The functions here do extract and load only. Data lands in the `raw` schema
as-is: no cleaning, filtering or renaming. The one exception is that column
names are lowercased, because TLC files spell the same column differently
across months (for example `airport_fee` and `Airport_fee`). All business
logic belongs in dbt.

The module does not depend on Airflow, so it can be reused or tested on its
own. Database settings come from the WAREHOUSE_* environment variables, the
same ones dbt reads.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests
from psycopg import sql

log = logging.getLogger(__name__)

TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

RAW_SCHEMA = "raw"
TRIPS_TABLE = "yellow_trips"
ZONES_TABLE = "taxi_zones"
LOAD_LOG_TABLE = "ingestion_log"

# Columns every monthly file must have (lowercased) for downstream models to work.
REQUIRED_TRIP_COLUMNS = {
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
}

ZONE_COLUMNS = ["locationid", "borough", "zone", "service_zone"]

# Known columns and their raw types. Numeric types are kept loose on purpose
# (TLC switches some columns between int and float); staging does the casting.
# Columns that appear in future files are added automatically, see
# _add_missing_columns.
TRIP_COLUMNS = {
    "vendorid": "bigint",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double precision",
    "trip_distance": "double precision",
    "ratecodeid": "double precision",
    "store_and_fwd_flag": "text",
    "pulocationid": "bigint",
    "dolocationid": "bigint",
    "payment_type": "bigint",
    "fare_amount": "double precision",
    "extra": "double precision",
    "mta_tax": "double precision",
    "tip_amount": "double precision",
    "tolls_amount": "double precision",
    "improvement_surcharge": "double precision",
    "total_amount": "double precision",
    "congestion_surcharge": "double precision",
    "airport_fee": "double precision",
    "cbd_congestion_fee": "double precision",
}

DOWNLOAD_CHUNK_BYTES = 1024 * 1024
LOAD_BATCH_ROWS = 100_000


class FileNotPublishedError(Exception):
    """The requested file does not exist at the source (yet)."""


class ValidationError(Exception):
    """A downloaded file does not have the expected shape."""


# ---------------------------------------------------------------------------
# Months and file names
# ---------------------------------------------------------------------------

def shift_month(year_month: str, months: int) -> str:
    """Shift a 'YYYY-MM' string by a number of months (negative = earlier)."""
    year, month = (int(part) for part in year_month.split("-"))
    index = year * 12 + (month - 1) + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def trip_file_name(year_month: str) -> str:
    return f"yellow_tripdata_{year_month}.parquet"


def trip_file_url(year_month: str) -> str:
    return TRIP_URL_TEMPLATE.format(year_month=year_month)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(url: str, dest: str | Path) -> Path:
    """Stream a file to disk, replacing any existing copy only once complete.

    Raises FileNotPublishedError if the source has no such file. The TLC CDN
    answers 403 rather than 404 for missing files, so both count as missing.
    Other HTTP and network errors are raised as-is so the caller can retry.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_name(dest.name + ".part")

    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=(10, 120)) as response:
        if response.status_code in (403, 404):
            raise FileNotPublishedError(
                f"{url} is not available (HTTP {response.status_code}). "
                "TLC publishes each month with a delay of about two months."
            )
        response.raise_for_status()
        with open(partial, "wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                handle.write(chunk)

    os.replace(partial, dest)
    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
    return dest


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_trip_file(path: str | Path) -> int:
    """Check a monthly Parquet file and return its row count."""
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:  # pyarrow raises several types for corrupt files
        raise ValidationError(f"{path} is not a readable Parquet file: {exc}") from exc

    columns = {name.lower() for name in parquet.schema_arrow.names}
    missing = REQUIRED_TRIP_COLUMNS - columns
    if missing:
        raise ValidationError(f"{path} is missing required columns: {sorted(missing)}")

    rows = parquet.metadata.num_rows
    if rows == 0:
        raise ValidationError(f"{path} contains no rows")

    unknown = columns - TRIP_COLUMNS.keys()
    if unknown:
        log.warning("New columns in %s will be added to the raw table: %s", path, sorted(unknown))

    log.info("%s is valid: %d rows, %d columns", path, rows, len(columns))
    return rows


def validate_zone_file(path: str | Path) -> None:
    """Check that the zone lookup CSV has the expected header."""
    with open(path, encoding="utf-8-sig") as handle:
        header = handle.readline()
    columns = [col.strip().strip('"').lower() for col in header.split(",")]
    if columns != ZONE_COLUMNS:
        raise ValidationError(f"Unexpected zone lookup header in {path}: {columns}")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def connect() -> psycopg.Connection:
    """Open an autocommit connection to the warehouse from WAREHOUSE_* env vars.

    Autocommit is on so that each `with conn.transaction():` block is exactly
    one transaction.
    """
    return psycopg.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
        autocommit=True,
    )


def _table(name: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(RAW_SCHEMA), sql.Identifier(name))


def _ensure_raw_tables(cur: psycopg.Cursor) -> None:
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(RAW_SCHEMA)))

    column_defs = [
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg_type))
        for name, pg_type in TRIP_COLUMNS.items()
    ]
    column_defs += [
        sql.SQL("source_file text NOT NULL"),
        sql.SQL("loaded_at timestamptz NOT NULL DEFAULT now()"),
    ]
    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            _table(TRIPS_TABLE), sql.SQL(", ").join(column_defs)
        )
    )
    for column in ("tpep_pickup_datetime", "pulocationid", "dolocationid", "source_file"):
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(f"{TRIPS_TABLE}_{column}_idx"),
                _table(TRIPS_TABLE),
                sql.Identifier(column),
            )
        )

    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                locationid   integer,
                borough      text,
                zone         text,
                service_zone text,
                loaded_at    timestamptz NOT NULL DEFAULT now()
            )
            """
        ).format(_table(ZONES_TABLE))
    )

    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                source_file  text        NOT NULL,
                target_table text        NOT NULL,
                rows_deleted bigint      NOT NULL,
                rows_loaded  bigint      NOT NULL,
                loaded_at    timestamptz NOT NULL DEFAULT now()
            )
            """
        ).format(_table(LOAD_LOG_TABLE))
    )


def _pg_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_integer(arrow_type):
        return "bigint"
    if pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
        return "double precision"
    if pa.types.is_timestamp(arrow_type):
        return "timestamptz" if arrow_type.tz else "timestamp"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    return "text"


def _add_missing_columns(cur: psycopg.Cursor, schema: pa.Schema) -> None:
    """Add any file column the raw table does not have yet (schema drift)."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (RAW_SCHEMA, TRIPS_TABLE),
    )
    existing = {row[0] for row in cur.fetchall()}
    for field in schema:
        name = field.name.lower()
        if name not in existing:
            pg_type = _pg_type(field.type)
            log.warning("Adding new column %s %s to %s.%s", name, pg_type, RAW_SCHEMA, TRIPS_TABLE)
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    _table(TRIPS_TABLE), sql.Identifier(name), sql.SQL(pg_type)
                )
            )


def load_trip_file(conn: psycopg.Connection, path: str | Path, expected_rows: int) -> int:
    """Load one monthly Parquet file into raw.yellow_trips and return the row count.

    Idempotent: the month's existing rows (matched on source_file) are deleted
    first. Delete and insert run in one transaction, so a failure part-way
    leaves the previous data in place. The file is streamed in batches and
    sent with COPY, so a full month is never held in memory at once.
    """
    path = Path(path)
    source_file = path.name
    parquet = pq.ParquetFile(path)
    columns = [name.lower() for name in parquet.schema_arrow.names]

    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
        _table(TRIPS_TABLE),
        sql.SQL(", ").join(sql.Identifier(c) for c in [*columns, "source_file"]),
    )
    csv_options = pacsv.WriteOptions(include_header=False)

    with conn.transaction(), conn.cursor() as cur:
        _ensure_raw_tables(cur)
        _add_missing_columns(cur, parquet.schema_arrow)

        cur.execute(
            sql.SQL("DELETE FROM {} WHERE source_file = %s").format(_table(TRIPS_TABLE)),
            (source_file,),
        )
        rows_deleted = cur.rowcount
        if rows_deleted:
            log.info("Deleted %d existing rows for %s", rows_deleted, source_file)

        rows_loaded = 0
        with cur.copy(copy_sql) as copy:
            for batch in parquet.iter_batches(batch_size=LOAD_BATCH_ROWS):
                batch = batch.rename_columns(columns)
                batch = batch.append_column(
                    "source_file", pa.array([source_file] * batch.num_rows, pa.string())
                )
                buffer = io.BytesIO()
                pacsv.write_csv(batch, buffer, csv_options)
                copy.write(buffer.getvalue())
                rows_loaded += batch.num_rows
                log.info("Copied %d / %d rows", rows_loaded, expected_rows)

        # Raising here rolls back the whole load, including the delete.
        if rows_loaded != expected_rows:
            raise ValidationError(
                f"Loaded {rows_loaded} rows from {source_file}, expected {expected_rows}"
            )

        cur.execute(
            sql.SQL(
                "INSERT INTO {} (source_file, target_table, rows_deleted, rows_loaded) "
                "VALUES (%s, %s, %s, %s)"
            ).format(_table(LOAD_LOG_TABLE)),
            (source_file, f"{RAW_SCHEMA}.{TRIPS_TABLE}", rows_deleted, rows_loaded),
        )

    log.info("Loaded %d rows from %s into %s.%s", rows_loaded, source_file, RAW_SCHEMA, TRIPS_TABLE)
    return rows_loaded


def load_zone_file(conn: psycopg.Connection, path: str | Path) -> int:
    """Replace the contents of raw.taxi_zones with the lookup CSV."""
    path = Path(path)
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        _table(ZONES_TABLE),
        sql.SQL(", ").join(sql.Identifier(c) for c in ZONE_COLUMNS),
    )

    with conn.transaction(), conn.cursor() as cur:
        _ensure_raw_tables(cur)
        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(_table(ZONES_TABLE)))
        rows_deleted = cur.fetchone()[0]
        cur.execute(sql.SQL("TRUNCATE {}").format(_table(ZONES_TABLE)))

        with open(path, "rb") as handle, cur.copy(copy_sql) as copy:
            while chunk := handle.read(DOWNLOAD_CHUNK_BYTES):
                copy.write(chunk)

        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(_table(ZONES_TABLE)))
        rows_loaded = cur.fetchone()[0]
        if rows_loaded == 0:
            raise ValidationError(f"{path} produced no rows")

        cur.execute(
            sql.SQL(
                "INSERT INTO {} (source_file, target_table, rows_deleted, rows_loaded) "
                "VALUES (%s, %s, %s, %s)"
            ).format(_table(LOAD_LOG_TABLE)),
            (path.name, f"{RAW_SCHEMA}.{ZONES_TABLE}", rows_deleted, rows_loaded),
        )

    log.info("Loaded %d zones into %s.%s", rows_loaded, RAW_SCHEMA, ZONES_TABLE)
    return rows_loaded
