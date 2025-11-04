FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir dbt-core==1.10.0 dbt-duckdb duckdb

ENTRYPOINT ["/bin/bash"]