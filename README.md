# Natural Events Real-Time Data Pipeline

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.2-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-1.27.0-003366?style=for-the-badge&logo=apache-nifi&logoColor=white)](https://nifi.apache.org)
[![Apache Parquet](https://img.shields.io/badge/Apache%20Parquet-latest-50A0FF?style=for-the-badge&logo=apache&logoColor=white)](https://parquet.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.10.0-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Docker](https://img.shields.io/badge/Docker-latest-0db7ed?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1.0-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org)
[![Google BigQuery](https://img.shields.io/badge/BigQuery-latest-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Redpanda](https://img.shields.io/badge/Redpanda-24.2.7-FF3C00?style=for-the-badge&logo=redpanda&logoColor=white)](https://redpanda.com)

A streaming data pipeline that tracks natural disasters around the world in real-time. This project ingests fire hotspot data from NASA and earthquake data from USGS, processes it through a message broker, transforms it with Spark and dbt, orchestrates data workflows with Airflow, and exports analytics to Google BigQuery.

## What This Does

1. Pulls the latest fire and earthquake data from public APIs (NASA FIRMS & USGS)
2. Streams that data through Kafka (via Redpanda)
3. Processes it with Spark Structured Streaming
4. Stores it in partitioned Parquet files
5. Transforms data with dbt (staging views + aggregated marts)
6. Orchestrates hourly pipelines with Airflow (powered by PostgreSQL)
7. Exports analytics to Google BigQuery via GCS for dashboards and reporting

## Architecture

<image title="ETL Architecture" src="architecture/natural_events_architecture.png">

**Components:**

- **NiFi**: Data ingestion from NASA FIRMS and USGS APIs
- **Redpanda**: Kafka-compatible message broker
- **Spark**: Stream processing with structured streaming
- **Parquet**: Column-oriented storage format (partitioned by date)
- **DuckDB**: Analytical database for querying Parquet files
- **dbt**: Data transformation using SQL (staging + marts)
- **Airflow**: Pipeline orchestration and scheduling (hourly DAGs)
- **PostgreSQL**: Airflow metadata database
- **Google Cloud Storage**: Intermediate storage for BigQuery loads
- **BigQuery**: Cloud data warehouse for analytics and dashboards

## Prerequisites

- Docker and Docker Compose
- NASA FIRMS API key (https://firms.modaps.eosdis.nasa.gov/api/)
- Google Cloud Platform account with:
  - Service account credentials (JSON file)
  - BigQuery API enabled
  - Cloud Storage bucket created
  - Appropriate IAM permissions for BigQuery and GCS
- M1/M2 Mac, or adjust the Docker images for your architecture

## Project Structure

```
natural-disasters-pipeline/
├── docker-compose.yml              # All services (NiFi, Redpanda, Spark, Airflow, PostgreSQL)
├── Dockerfile                      # Custom Airflow image with dbt and GCP dependencies
├── spark-apps/
│   └── streaming_app.py           # Spark Structured Streaming job
├── spark-data/
│   ├── fires/                     # Fire data Parquet files (partitioned by acq_date)
│   ├── earthquakes/               # Earthquake Parquet files (partitioned by event_date)
│   ├── checkpoints/               # Spark streaming checkpoints for fault tolerance
│   ├── exports/                   # Temporary Parquet exports for BigQuery
│   └── natural_events.duckdb      # DuckDB database file
├── airflow/
│   ├── dags/
│   │   └── migrate_duckdb_bq.py  # Hourly DAG for dbt transformation + BigQuery export
│   ├── scripts/
│   │   └── export_to_bq.py       # Script to export DuckDB to BigQuery via GCS
│   ├── logs/                     # Airflow execution logs
│   ├── airflow.cfg               # Airflow configuration
│   └── webserver_config.py       # Airflow webserver settings
├── dbt/
│   ├── models/
│   │   ├── staging/              # Staging views (stg_fires, stg_earthquakes)
│   │   │   └── sources.yml       # Source definitions for Parquet files
│   │   └── marts/                # Aggregated tables (fire_summary, earthquake_summary, combined_events)
│   ├── dbt_project.yml           # dbt project configuration
│   └── profiles.yml              # DuckDB connection config
├── config/
│   └── gcp-credentials.json      # GCP service account credentials
└── architecture/
    └── natural_events_architecture.png  # Architecture diagram
```

## Getting Started

### 1. Clone and Setup

```bash
git clone <your-repo>
cd natural-disasters-pipeline

# Create necessary directories
mkdir -p spark-apps spark-data/fires spark-data/earthquakes spark-data/checkpoints spark-data/exports
mkdir -p airflow/dags airflow/logs airflow/scripts
mkdir -p dbt/models/staging dbt/models/marts
mkdir -p config

# Add your GCP credentials
cp /path/to/your/credentials.json config/gcp-credentials.json
```

### 2. Start the Services

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL** (Airflow metadata database)
- **NiFi** (https://localhost:8443) - Data ingestion
- **Redpanda** + **Redpanda Console** (http://localhost:8090) - Message broker
- **Spark Master** (http://localhost:8070) + **Spark Worker** - Stream processing
- **dbt** - Data transformation container
- **Airflow Webserver** (http://localhost:8080) - Orchestration UI (admin/admin)
- **Airflow Scheduler** - DAG execution

Wait a few minutes for all services to initialize. PostgreSQL must be healthy before Airflow starts.

### 3. Configure NiFi

Access NiFi at https://localhost:8443/nifi (username: `admin`, password: `adminadminadmin`)

**NASA FIRMS flow:**

WIP. Possible template or just general guidelines

### 4. Create Redpanda Topics

```bash
docker exec -it redpanda rpk topic create fires-topic -c max.message.bytes=5242880
docker exec -it redpanda rpk topic create earthquakes-topic -c max.message.bytes=5242880
```

### 5. Run Spark Streaming

Submit the Spark streaming job:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /opt/spark-apps/streaming_app.py
```

The job will:
- Consume messages from both Redpanda topics
- Parse CSV (fires) and JSON (earthquakes)
- Write to Parquet files every 30 seconds
- Partition fire data by acquisition date
- Use watermarks to handle late-arriving data
- Maintain checkpoints for fault tolerance

### 6. Configure and Run Airflow

Access Airflow UI at http://localhost:8080 (username: `admin`, password: `admin`)

**Airflow Configuration:**
- **Executor**: LocalExecutor (supports parallel task execution)
- **Database**: PostgreSQL 15 (`postgresql+psycopg2://airflow:airflow@postgres/airflow`)
- **DAG Schedule**: `@hourly` (runs every hour)

**The `migrate_duckdb_to_bigquery` DAG orchestrates:**
1. **check_dbt_path** - Verify dbt installation
2. **setup_dbt_temp_directory** - Create temporary directory for dbt run
3. **run_dbt_models** - Execute dbt transformations on mart models
   - Staging views (`stg_fires`, `stg_earthquakes`) that clean and filter raw Parquet data
   - Summary tables with aggregations by date, satellite, and confidence level
   - A combined events table with both fires and earthquakes
4. **test_dbt_models** - Run dbt tests for data quality checks
5. **cleanup_dbt_temp_directory** - Remove temporary files
6. **run_export_script** - Export DuckDB tables to BigQuery via GCS

**BigQuery Export Process:**
- Reads tables from DuckDB: `fire_summary`, `earthquake_summary`, `combined_events`
- Exports to Parquet files locally
- Uploads Parquet to GCS bucket (`natural-events-staging`)
- Loads Parquet from GCS into BigQuery (`natural-events-pipeline.natural_events`)
- Deletes temporary GCS files

Enable the DAG in the Airflow UI to start scheduled hourly runs.

## Understanding the Data Flow

**Fires Data:**
- Updates every 15 minutes from NASA FIRMS
- CSV format with latitude, longitude, brightness, confidence, fire radiative power
- Partitioned by acquisition date in Parquet

**Earthquakes Data:**
- Updates every minute from USGS
- GeoJSON format with magnitude, location, depth, time
- All earthquakes from the past hour

**Processing Pipeline:**

```
APIs → NiFi → Redpanda → Spark Streaming → Parquet → DuckDB + dbt → BigQuery
```

1. **Spark Streaming** (Real-time processing)
   - Reads from Kafka topics in micro-batches (30-second intervals)
   - Applies data cleaning and validation
   - Uses `from_csv` for NASA FIRMS data to handle embedded newlines
   - Converts date strings to proper date types
   - Writes to Parquet with no compression for ARM64 compatibility
   - Fire data partitioned by `acq_date`, earthquakes by `event_date`
   - Checkpoints ensure fault tolerance and exactly-once semantics

2. **dbt Transformations** (Batch processing)
   - **Staging layer**: Creates clean views from raw Parquet files
   - **Marts layer**: Aggregates data into analysis-ready tables
   - Runs via Airflow on an hourly schedule

3. **Airflow Orchestration** (Hourly)
   - Executes dbt models to transform data
   - Runs data quality tests
   - Exports DuckDB tables to BigQuery via GCS
   - Maintains execution logs and metadata in PostgreSQL

4. **BigQuery Export**
   - Tables loaded hourly with `WRITE_TRUNCATE` mode
   - Ready for dashboards, analytics, and reporting

## Technical Highlights

**Airflow with PostgreSQL:**
- **LocalExecutor** enables parallel task execution
- **PostgreSQL 15** provides robust metadata storage (replacing SQLite)
- Persistent storage with Docker volume (`postgres-airflow`)
- Health checks ensure database availability before Airflow initialization
- Supports concurrent task execution and better performance

**CSV Parsing:**
- Uses `from_csv` with defined schema for robust parsing
- Handles embedded newlines in NASA FIRMS data
- Filters out header rows and empty lines

**Data Partitioning:**
- Fire data partitioned by `acq_date` (acquisition date)
- Earthquake data partitioned by `event_date`
- Directory structure: `/fires/acq_date=2025-11-05/`, `/earthquakes/event_date=2025-11-05/`
- Enables efficient time-based queries
- DuckDB can read specific partitions when filtering by date

**Stream Processing:**
- RocksDB state store for Spark streaming checkpoints
- Adaptive query execution for dynamic query optimization
- Watermarks prevent unbounded state growth (1 hour for fires, 10 min for earthquakes)
- `foreachBatch` provides batch-level monitoring
- Fault tolerance with checkpoint directories

**dbt Data Models:**
- **Sources**: Defines Parquet file locations for DuckDB to read
- **Staging views**: Clean and standardize raw data (`stg_fires`, `stg_earthquakes`)
- **Mart tables**: Aggregated analytics-ready tables
  - `fire_summary`: Grouped by timestamp, satellite, confidence
  - `earthquake_summary`: Grouped by hour with magnitude/depth stats
  - `combined_events`: Unified dataset of all natural events

**BigQuery Integration:**
- Concurrent export of 3 tables using ThreadPoolExecutor
- Automatic dataset creation with US location
- Retry logic with exponential backoff (max 5 attempts)
- Schema autodetection from Parquet files
- Intermediate staging via GCS for efficient loads

## Common Issues

**NiFi can't publish to Redpanda:**
- Check KafkaConnectionService uses `redpanda:9092`
- Verify both containers are on `pipeline-network`
- Increase message size with rpk topic alter-config if needed

**Spark job fails with Snappy compression error:**
- M1/M2 Mac (ARM64) issue with native compression libraries
- Use `option("compression", "none")` to work around

**No data in Parquet files:**
- Check if NiFi is fetching data
- Check if Redpanda topics have messages (Redpanda Console)
- Check Spark logs for errors

**dbt can't find tables:**
- Ensure Spark has written Parquet files to spark-data/
- Verify paths in dbt models match actual file locations

**Airflow DAG fails:**
- Check PostgreSQL container is healthy: `docker ps`
- Verify GCP credentials are mounted: `/opt/config/gcp-credentials.json`
- Check environment variables are set correctly in docker-compose.yml
- Review Airflow logs: `docker logs airflow-webserver` or `docker logs airflow-scheduler`

**BigQuery export fails:**
- Verify GCS bucket exists and service account has write permissions
- Check BigQuery dataset exists or service account can create datasets
- Ensure GCP credentials JSON is valid
- Review export script logs in Airflow task instance

## What's Next

1. Connect Looker Studio to BigQuery for visualization dashboards
2. Implement incremental dbt models for better performance
3. Add data quality tests in dbt (uniqueness, freshness, accepted values)
4. Split large messages in NiFi using SplitText processor
5. Add deduplication for duplicate events
6. Add alerting for major events (magnitude > 6 earthquakes, large fires)
7. Implement data retention policies (archival to cold storage)
8. Add monitoring and observability (Prometheus + Grafana)
9. Scale Airflow with CeleryExecutor and Redis for distributed task execution

## Tech Stack Summary

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Airflow | 2.10.2 | Pipeline orchestration and scheduling |
| PostgreSQL | 15-alpine | Airflow metadata database |
| Apache NiFi | 1.27.0 | Data ingestion from APIs |
| Redpanda | 24.2.7 | Message broker (Kafka-compatible) |
| Apache Spark | 3.4.0 | Stream processing (Structured Streaming) |
| dbt-core | 1.8.0 | Data transformation |
| dbt-duckdb | 1.8.4 | DuckDB adapter for dbt |
| DuckDB | 1.1.0 | Analytical database |
| Google BigQuery | - | Cloud data warehouse |
| Google Cloud Storage | - | Intermediate storage for BigQuery loads |
| Apache Parquet | - | Columnar storage format |
| Docker | - | Containerization |

## Resources

- [NASA FIRMS API Docs](https://firms.modaps.eosdis.nasa.gov/api/)
- [USGS Earthquake API Docs](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Redpanda Documentation](https://docs.redpanda.com/)
- [Google BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)

## Authors

Jed Lanelle
* Contact: jed.lanelle@gmail.com
* Github: [Jedaian](https://github.com/jedaian)
* LinkedIn: [JedLanelle](https://linkedin.com/in/jedlanelle)

## License

MIT License. See the [LICENSE](LICENSE) file for details.