FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-duckdb \
    google-cloud-bigquery \
    pandas \
    pyarrow

#Install Bruin CLI using official script
RUN curl -LsSf https://getbruin.com/install/cli | sh && \
    mv /root/.local/bin/bruin /usr/local/bin/bruin

WORKDIR /workspace

CMD ["tail", "-f", "/dev/null"]