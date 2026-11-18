# Custom Airflow image: Airflow + ingestion libraries + dbt.
FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
ARG DBT_CORE_VERSION=1.12.6
ARG DBT_POSTGRES_VERSION=1.11.0

# Ingestion libraries, pinned against Airflow's official constraints so they
# cannot break Airflow's own dependencies.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt lives in its own virtualenv: its dependency pins clash with Airflow's.
# The venv's bin is appended to PATH, so `dbt` is callable from tasks without
# shadowing Airflow's Python.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-postgres==${DBT_POSTGRES_VERSION}"

ENV PATH="${PATH}:/home/airflow/dbt_venv/bin"
