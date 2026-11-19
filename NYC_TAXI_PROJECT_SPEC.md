# NYC Taxi Analytics Pipeline

**Project overview and specification**

An end-to-end ELT pipeline on real NYC yellow taxi trip data. Apache Airflow ingests monthly Parquet files into PostgreSQL, dbt transforms the raw data into tested analytics models (including incremental models), and Metabase serves dashboards on top.

| | |
|---|---|
| **Repository** | `nyc-taxi-dbt-pipeline` |
| **Owner** | Jeremiah Anku Coblah |
| **Status** | Planning |
| **Type** | Portfolio / learning project |

---

## 1. Overview

### 1.1 Purpose

Build a production-style analytics engineering project that handles real data at volume. It follows on from the Olist dbt project, which used a small relational dataset. This project is larger and time-based, so it requires dbt features that Olist did not: incremental models, packages, source freshness, and reusable macros. It also adds orchestration with Airflow.

### 1.2 Business scenario

The project plays the role of an analytics engineer at a transport analytics company. The business wants reliable answers to:

- When and where is taxi demand highest?
- Which zones and routes generate the most trips?
- How much revenue comes from airport trips?
- How do tip percentages vary by hour, day, and trip type?
- How do fares, distances, and speeds change month to month?

### 1.3 Goals

1. Automate monthly ingestion of raw trip data with Airflow.
2. Transform raw data into clean, tested, documented models with dbt.
3. Process new data incrementally instead of rebuilding history.
4. Enforce data quality with tests, freshness checks, and documented cleaning rules.
5. Deliver a dashboard that answers the business questions above.
6. Make the whole stack reproducible with Docker Compose.

### 1.4 Non-goals

- Real-time or streaming ingestion (the data is published monthly).
- Machine learning or forecasting.
- Green taxi and for-hire vehicle datasets (possible future extension).
- Cloud deployment (the project runs locally in Docker).

---

## 2. Data Sources

### 2.1 Yellow taxi trip records

- **Publisher:** NYC Taxi & Limousine Commission (TLC)
- **Landing page:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Format:** one Parquet file per month
- **URL pattern:** `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY-MM}.parquet`
- **Volume:** roughly millions of trips per month
- **Data dictionary:** https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

**Key columns:**

| Column | Description |
|---|---|
| `VendorID` | Technology provider that supplied the record |
| `tpep_pickup_datetime` | Meter engaged |
| `tpep_dropoff_datetime` | Meter disengaged |
| `passenger_count` | Driver-reported passengers (often null) |
| `trip_distance` | Distance in miles |
| `RatecodeID` | Rate type: standard, JFK, Newark, negotiated, and so on |
| `PULocationID`, `DOLocationID` | Pickup and drop-off taxi zone IDs |
| `payment_type` | Numeric payment code |
| `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `total_amount` | Itemised charges |
| `airport_fee` | Airport charge (column capitalisation differs between files) |
| `cbd_congestion_fee` | Present in 2025 files onward only |

### 2.2 Taxi zone lookup

- **Format:** CSV
- **Columns:** `LocationID`, `Borough`, `Zone`, `service_zone`
- **Source:** linked on the TLC page under "Taxi Zone Maps and Lookup Tables"
- **Common direct link:** `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` (fall back to the TLC page link if it does not download)

### 2.3 Known data issues

These are real and must be handled, not ignored:

| Issue | Handling |
|---|---|
| Negative or zero fares | Filter in staging |
| Zero-distance trips and extreme outliers | Filter using documented bounds |
| Drop-off earlier than pickup | Filter in staging |
| Pickup timestamps outside the file's month | Filter or flag (decision recorded in README) |
| Null `passenger_count` | Keep the row, leave null, do not impute |
| **Cash tips are not recorded** | Compute tip metrics on card payments only |
| Schema drift between months | Staging model tolerates missing or renamed columns |
| No unique trip ID | Generate a surrogate key |
| Possible duplicate rows | Deduplicate on the surrogate key |

---

## 3. Architecture

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

**Division of responsibility:**

- **Extract and Load:** Airflow and Python.
- **Transform:** dbt only. All business logic and cleaning rules live in dbt.
- **Serve:** Metabase reads only from the marts schema.

---

## 4. Tech Stack

| Component | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Warehouse | PostgreSQL |
| Transformation | dbt Core with `dbt-postgres` |
| dbt packages | `dbt_utils`, `dbt_expectations` |
| Ingestion code | Python (`pandas`, `pyarrow`, `sqlalchemy`) |
| BI | Metabase |
| Infrastructure | Docker and Docker Compose |
| Version control | Git, GitHub, feature branches |
| CI (optional) | GitHub Actions: `dbt build` on a small sample |

---

## 5. Repository Structure

```
nyc-taxi-dbt-pipeline/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── airflow/
│   ├── dags/
│   │   ├── ingest_yellow_trips.py
│   │   └── load_zone_lookup.py
│   └── plugins/
├── ingestion/
│   └── loaders.py                  # shared download and load functions
├── dbt_project/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml
│   │   │   ├── _staging.yml
│   │   │   ├── stg_yellow_trips.sql
│   │   │   └── stg_taxi_zones.sql
│   │   ├── intermediate/            # optional
│   │   └── marts/
│   │       ├── _marts.yml
│   │       ├── dim_zones.sql
│   │       ├── dim_payment_types.sql
│   │       ├── dim_rate_codes.sql
│   │       ├── fct_trips.sql
│   │       ├── hourly_demand_by_zone.sql
│   │       ├── avg_tip_by_time_of_day.sql
│   │       └── airport_trip_revenue.sql
│   ├── macros/
│   │   └── valid_trip_filter.sql
│   ├── seeds/
│   │   ├── payment_types.csv
│   │   └── rate_codes.csv
│   ├── snapshots/                   # optional
│   └── tests/
└── docs/
    ├── architecture.png
    └── lineage.png
```

`data/` and any `.parquet` files must be git-ignored.

---

## 6. Functional Specification

### 6.1 Ingestion (Airflow)

**DAG `ingest_yellow_trips`**

- **Schedule:** monthly, with manual trigger support and a configurable `year_month` parameter.
- **Steps:**
  1. Build the file URL from the month.
  2. Download the Parquet file to a temporary location.
  3. Read in batches and load to `raw.yellow_trips`.
  4. Add metadata columns: `source_file` and `loaded_at`.
  5. Record the row count loaded.
  6. Trigger the dbt build.
- **Idempotency:** re-running a month must delete that month's existing rows (by `source_file`) before inserting, so it never creates duplicates.
- **Failure handling:** retries with backoff, and a clear failure if the file does not exist yet.
- **Memory:** load in chunks. Never read a full month into one DataFrame and insert it in one statement.

**DAG `load_zone_lookup`**

- Runs once or on demand. Replaces the contents of `raw.taxi_zones` with the CSV.

**Raw table rules**

- Data is loaded as-is. No cleaning, filtering, or renaming at this layer.
- Index `tpep_pickup_datetime` and the location ID columns to keep dbt queries fast.

### 6.2 Staging (dbt)

**`stg_yellow_trips`**

- Rename columns to snake_case with clear names.
- Cast types explicitly.
- Handle schema drift (for example, `airport_fee` casing; `cbd_congestion_fee` absent in older files).
- Apply cleaning rules through the `valid_trip_filter` macro.
- Generate `trip_id` with `dbt_utils.generate_surrogate_key`.
- Deduplicate on `trip_id`.
- Materialised as a view, or as a table if performance requires it.

**`stg_taxi_zones`**

- Rename and trim. One row per `location_id`.

**Cleaning rules (initial, adjust and document in README):**

| Rule | Condition kept |
|---|---|
| Positive fare | `fare_amount > 0` |
| Positive distance | `trip_distance > 0` and below a sensible upper bound |
| Valid duration | drop-off after pickup, and under a sensible maximum |
| Valid dates | pickup within the expected month of the source file |
| Known zones | pickup and drop-off IDs exist in the zone lookup, or are flagged |

### 6.3 Marts (dbt)

**Dimensions**

| Model | Grain | Notes |
|---|---|---|
| `dim_zones` | one row per taxi zone | borough, zone name, service zone, `is_airport` flag |
| `dim_payment_types` | one row per payment code | from seed; readable labels |
| `dim_rate_codes` | one row per rate code | from seed; readable labels |

**Fact**

| Model | Grain | Materialisation |
|---|---|---|
| `fct_trips` | one row per trip | **incremental** |

Derived columns on `fct_trips`:

- `trip_duration_minutes`
- `avg_speed_mph`
- `tip_pct` (card payments only, otherwise null)
- `pickup_date`, `pickup_hour`, `pickup_day_of_week`, `is_weekend`
- `is_airport_trip`

**Reporting tables**

| Model | Grain | Answers |
|---|---|---|
| `hourly_demand_by_zone` | zone × date × hour | trips and revenue by time and place |
| `avg_tip_by_time_of_day` | hour × day of week | tip percentage on card trips |
| `airport_trip_revenue` | airport × month | trip count, revenue, average fare |

### 6.4 Incremental model specification

`fct_trips` must:

- Use a `unique_key` of `trip_id`.
- On incremental runs, process only rows newer than what already exists, or only the newly loaded month.
- Support a `--full-refresh` that rebuilds from scratch.
- Produce no duplicates after being run twice on the same data.
- Be tested by this sequence: load month 1, run, load month 2, run, then confirm that row count equals the sum of both months and that no `trip_id` appears twice.

### 6.5 Macros

| Macro | Purpose |
|---|---|
| `valid_trip_filter()` | One reusable block of cleaning conditions used by staging |
| (optional) `tip_percentage()` | Tip calculation, null for non-card payments |

### 6.6 Seeds

Small lookup CSVs in `seeds/` for payment types and rate codes, taken from the TLC data dictionary.

---

## 7. Data Quality Specification

### 7.1 Generic tests

- `unique` and `not_null` on `trip_id`, `location_id`, and all dimension keys.
- `relationships` from `fct_trips` pickup and drop-off zone IDs to `dim_zones`.
- `accepted_values` on payment type and rate code.

### 7.2 Package tests

Using `dbt_expectations` and `dbt_utils`:

- Fare, distance, and duration within expected ranges.
- Row count of `fct_trips` within an expected range per month.
- No future pickup dates.

### 7.3 Custom singular tests

- Dropoff is never earlier than pickup in `fct_trips`.
- Tip percentage is null whenever the payment type is not credit card.
- Row count of `fct_trips` for a month does not exceed the staging count for that month.

### 7.4 Source freshness

Configured on `raw.yellow_trips` using `loaded_at`. Warn and error thresholds are set so a missed monthly load is detected.

---

## 8. Dashboard Specification

**Tool:** Metabase, connected to the marts schema only.

| Panel | Source | Chart |
|---|---|---|
| Total trips and total revenue (KPI tiles) | `fct_trips` | Number |
| Demand heatmap, hour × weekday | `hourly_demand_by_zone` | Pivot table or conditional-format table |
| Top 10 pickup zones | `hourly_demand_by_zone` | Horizontal bar |
| Average tip % by hour | `avg_tip_by_time_of_day` | Line |
| Airport revenue by month | `airport_trip_revenue` | Stacked bar |
| Monthly trips and average fare | `fct_trips` | Combo or line |

**Filters:** date range, borough, payment type.

> Note: tip charts must be labelled "card payments only", since cash tips are not recorded.

---

## 9. Non-Functional Requirements

| Requirement | Detail |
|---|---|
| **Reproducibility** | `docker compose up -d` starts Postgres, Airflow, and Metabase. A new developer can run the project from the README. |
| **Idempotency** | Every ingestion and dbt run can be repeated safely. |
| **Secrets** | Credentials live in `.env` and `profiles.yml`, never committed. A `.env.example` is provided. |
| **Performance** | Indexes on the raw table, and the incremental fact model, keep a monthly run to a reasonable time on a laptop. |
| **Documentation** | Every model and key column has a description in YAML. `dbt docs` is generated. |
| **Scope control** | Begin with three months of data. Scale to a full year only once everything works. |

---

## 10. Milestones

| Phase | Focus | Done when |
|---|---|---|
| **1. Environment** | Docker Compose with Postgres, Airflow, Metabase. dbt connected. | `dbt debug` passes and the Airflow UI loads |
| **2. Ingestion** | Both DAGs, loading three months into `raw` | Row counts match the source files, and a re-run does not duplicate |
| **3. Staging** | `stg_yellow_trips`, `stg_taxi_zones`, macro, tests | Staging tests pass and dropped-row counts are documented |
| **4. Marts** | Dimensions, incremental `fct_trips`, reporting tables | Incremental test sequence in 6.4 passes |
| **5. Quality** | Packages, singular tests, freshness | `dbt build` is green and freshness is configured |
| **6. Dashboard** | Metabase panels and filters | All panels in section 8 render |
| **7. Polish** | README, lineage screenshot, findings, optional CI | Repo is portfolio-ready |

---

## 11. Deliverables Checklist

- [ ] Docker Compose stack (Postgres, Airflow, Metabase)
- [ ] Airflow DAG for monthly ingestion, idempotent and re-runnable
- [ ] Airflow DAG or task for the zone lookup
- [ ] dbt staging, dimension, fact, and reporting models
- [ ] One incremental model with a documented test procedure
- [ ] At least one reusable macro
- [ ] `dbt_utils` and `dbt_expectations` in use
- [ ] Generic, package, and singular tests
- [ ] Source freshness configured
- [ ] Seeds for payment and rate codes
- [ ] Metabase dashboard with the panels in section 8
- [ ] `dbt docs` generated, with lineage screenshot in the README
- [ ] README with architecture diagram, setup steps, cleaning rules, and 2 to 3 findings
- [ ] `.gitignore`, `.env.example`, and no secrets or data files in Git

---

## 12. Acceptance Criteria

The project is complete when:

1. A fresh clone runs end to end with the steps in the README.
2. Loading a new month and running the pipeline adds only that month's trips to `fct_trips`, with no duplicates.
3. `dbt build` completes with all tests passing.
4. The dashboard answers all five business questions in section 1.2.
5. The README documents the cleaning rules, the number of rows removed by each rule, and at least two findings drawn from the data.

---

## 13. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Files are large and slow to load | Start with three months, load in chunks, add indexes |
| Schema differs between months | Explicit column handling in staging, and a test that fails loudly on unexpected columns |
| Duplicate rows after re-running ingestion | Delete-then-insert by `source_file` |
| Incremental model drifts from a full refresh | Periodically compare incremental results with a `--full-refresh` build |
| Misleading tip statistics | Restrict tip analysis to card payments and label charts accordingly |
| Scope creep | Green taxi, FHV data, and cloud deployment stay out of scope until the core is done |

---

## 14. Possible Extensions

- Add green taxi data and union it with yellow.
- Add weather data and analyse its effect on demand.
- Add dbt snapshots on the zone lookup.
- Add a dbt exposure that documents the dashboard in the lineage graph.
- Deploy the stack to a cloud VM.
- Add GitHub Actions to run `dbt build` on a small sample on every pull request.
