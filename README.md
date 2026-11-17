# NYC Taxi Analytics Pipeline

An end-to-end ELT pipeline built on real NYC yellow taxi trip data. Apache Airflow ingests the monthly Parquet files into PostgreSQL. dbt transforms the raw data into tested analytics models, including incremental ones, and Metabase serves dashboards on top.

> **Status:** Early development. The repository structure is in place, and the pipeline itself is being built in phases (see [Roadmap](#roadmap)).

---

## Business questions

The project plays the role of an analytics engineer at a transport analytics company. The pipeline is designed to answer:

- When and where is taxi demand highest?
- Which zones and routes generate the most trips?
- How much revenue comes from airport trips?
- How do tip percentages vary by hour, day, and trip type?
- How do fares, distances, and speeds change month to month?

---

## Architecture

```
NYC TLC (monthly Parquet + zone CSV)
            │
            ▼
   Airflow DAG: download -> validate -> load
            │
            ▼
   PostgreSQL  schema: raw
     ├── raw.yellow_trips
     └── raw.taxi_zones
            │
            ▼  (Airflow triggers dbt after load)
   dbt
     ├── staging      (clean, rename, cast, filter)
     ├── intermediate (optional shared logic)
     └── marts        (dimensions, facts, reporting tables)
            │
            ▼
   Metabase dashboard
```

| Layer | Responsibility |
|---|---|
| Extract and load | Airflow and Python. Raw data is loaded as-is. |
| Transform | dbt only. All business logic and cleaning rules live here. |
| Serve | Metabase, reading only from the marts schema. |

---

## Tech stack

| Component | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Warehouse | PostgreSQL |
| Transformation | dbt Core (`dbt-postgres`) |
| dbt packages | `dbt_utils`, `dbt_expectations` |
| Ingestion | Python (`pandas`, `pyarrow`, `sqlalchemy`) |
| BI | Metabase |
| Infrastructure | Docker and Docker Compose |

---

## Data sources

- **Yellow taxi trip records**: monthly Parquet files published by the [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). See the [data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
- **Taxi zone lookup**: a CSV that maps `LocationID` to borough, zone, and service zone.

The project starts with three months of data and scales up once the pipeline is stable.

---

## Repository structure

```
nyc-taxi-dbt-pipeline/
├── airflow/
│   ├── dags/            # ingestion DAGs
│   └── plugins/
├── ingestion/           # shared download and load functions
├── dbt_project/
│   ├── models/
│   │   ├── staging/     # cleaned, renamed, typed source data
│   │   ├── intermediate/
│   │   └── marts/       # dimensions, fct_trips, reporting tables
│   ├── macros/          # reusable logic, e.g. valid_trip_filter
│   ├── seeds/           # payment type and rate code lookups
│   ├── snapshots/
│   └── tests/           # custom singular tests
└── docs/                # architecture and lineage diagrams
```

---

## Planned models

| Model | Type | Description |
|---|---|---|
| `stg_yellow_trips` | Staging | Cleaned trips with a surrogate `trip_id`, deduplicated |
| `stg_taxi_zones` | Staging | One row per taxi zone |
| `dim_zones` | Dimension | Zones with an `is_airport` flag |
| `dim_payment_types` | Dimension | Readable payment labels (from seed) |
| `dim_rate_codes` | Dimension | Readable rate code labels (from seed) |
| `fct_trips` | Fact (incremental) | One row per trip with duration, speed, tip %, and time attributes |
| `hourly_demand_by_zone` | Reporting | Trips and revenue by zone, date, and hour |
| `avg_tip_by_time_of_day` | Reporting | Card tip % by hour and day of week |
| `airport_trip_revenue` | Reporting | Airport trip count, revenue, and average fare by month |

---

## Data quality

Known issues in the source data are handled in the staging layer rather than ignored:

| Issue | Handling |
|---|---|
| Negative or zero fares | Filtered |
| Zero-distance trips and extreme outliers | Filtered using documented bounds |
| Drop-off earlier than pickup | Filtered |
| Pickups outside the file's month | Filtered or flagged |
| Null `passenger_count` | Kept as null, not imputed |
| Cash tips not recorded | Tip metrics use card payments only |
| Schema drift between months | Staging tolerates missing or renamed columns |
| No unique trip ID | Surrogate key generated and used to deduplicate |

Testing will cover generic tests (`unique`, `not_null`, `relationships`, `accepted_values`), `dbt_expectations` range checks, custom singular tests, and source freshness on the raw trips table.

The number of rows removed by each cleaning rule will be documented here once staging is built.

---

## Roadmap

- [x] Repository structure
- [ ] **Environment**: Docker Compose with Postgres, Airflow, and Metabase; dbt connected
- [ ] **Ingestion**: idempotent monthly trips DAG and zone lookup DAG
- [ ] **Staging**: staging models, `valid_trip_filter` macro, and tests
- [ ] **Marts**: dimensions, incremental `fct_trips`, and reporting tables
- [ ] **Quality**: package tests, singular tests, and source freshness
- [ ] **Dashboard**: Metabase panels and filters
- [ ] **Polish**: lineage screenshot, findings, and optional CI

---

## Getting started

Setup instructions will be added when the Docker Compose environment is in place. The goal is for the whole stack to start with:

```bash
docker compose up -d
```
