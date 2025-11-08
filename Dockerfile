FROM apache/airflow:2.10.2

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-duckdb==1.8.4 \
    google-cloud-storage==2.18.2 \
    google-cloud-bigquery==3.26.0 \
    pandas==2.2.3 \
    pyarrow==17.0.0 \
    db-dtypes==1.3.0 \
    duckdb==1.1.0

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__PARALLELISM=4 \
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2 \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False \
    AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW=graph \
    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=60 \
    AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30 \
    AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth \
    AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120 \
    AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL=30 \
    AIRFLOW__CORE__MIN_SERIALIZED_DAG_FETCH_INTERVAL=10 \
    AIRFLOW__SCHEDULER__PARSING_PROCESSES=2