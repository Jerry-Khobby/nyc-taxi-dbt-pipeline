#!/bin/bash
# Runs once, when the Postgres data volume is first created.
# The warehouse database (POSTGRES_DB) is created by the image itself.
# This adds separate databases and users for Airflow and Metabase metadata,
# and the raw schema the ingestion DAGs load into.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${AIRFLOW_DB_USER} WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER ${AIRFLOW_DB_USER};

    CREATE USER ${METABASE_DB_USER} WITH PASSWORD '${METABASE_DB_PASSWORD}';
    CREATE DATABASE metabase OWNER ${METABASE_DB_USER};

    CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION ${POSTGRES_USER};
EOSQL
