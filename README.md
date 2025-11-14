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

A streaming data pipeline that tracks natural disasters around the world in real-time. I learned a lot about NiFi, Redpanda, and Spark working on this, especially around near real-time data processing. The pipeline ingests fire hotspot data from NASA and earthquake data from USGS, streams it through Redpanda, processes it with Spark, and loads it into BigQuery for dashboards.

## What This Does

1. Pulls the latest fire and earthquake data from public APIs (NASA FIRMS & USGS)
2. Streams that data through Redpanda (Kafka-compatible message broker)
3. Processes it with Spark Structured Streaming every 15 minutes (includes spatial clustering for fires)
4. Stores it in partitioned Parquet files
5. Transforms data with dbt (staging views + aggregated marts)
6. Orchestrates hourly pipelines with Airflow and cleans up old data daily
7. Exports analytics to Google BigQuery via GCS (with partitioning and clustering)
8. Creates views and materialized views automatically for Looker Studio dashboards

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
├── start_pipeline.sh               # Custom script to automate docker, Nifi and Spark Jobs
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
│   │   ├── migrate_duckdb_bq.py  # Hourly DAG for dbt transformation + BigQuery export + MV creation
│   │   ├── create_mv_onetime.py  # One-time DAG to create materialized views manually
│   │   └── data_retention.py     # Daily DAG for cleaning up old Parquet partitions
│   ├── scripts/
│   │   ├── export_to_bq.py       # Script to export DuckDB to BigQuery via GCS
│   │   ├── create_mv.py          # Script to create BigQuery materialized views for Looker
│   │   └── cleanup_partitions.py # Script to remove old partitions (7-day retention)
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
- Write to Parquet files every 15 minutes
- Partition fire data by acquisition date
- Use watermarks to handle late-arriving data
- Maintain checkpoints for fault tolerance
- Run on a 1-worker Spark cluster (1GB memory per worker)

### 6. Configure and Run Airflow

Access Airflow UI at http://localhost:8080 (username: `admin`, password: `admin`)

**Airflow Configuration:**
- **Executor**: LocalExecutor (supports parallel task execution)
- **Database**: PostgreSQL 15 (`postgresql+psycopg2://airflow:airflow@postgres/airflow`)
- **DAG Schedule**: `@hourly` (runs every hour)

**Available DAGs:**

1. **`migrate_duckdb_to_bigquery`** (Hourly scheduled DAG):
   - **check_dbt_path** - Verify dbt installation
   - **setup_dbt_temp_directory** - Create temporary directory for dbt run
   - **run_dbt_models** - Execute dbt transformations on mart models
     - Staging views (`stg_fires`, `stg_earthquakes`) that clean and filter raw Parquet data
     - Summary tables with aggregations by date, satellite, and confidence level
     - A combined events table with both fires (clustered) and earthquakes
   - **test_dbt_models** - Run dbt tests for data quality checks
   - **cleanup_dbt_temp_directory** - Remove temporary files
   - **run_export_script** - Export DuckDB tables to BigQuery via GCS
   - **create_materialized_views** - Create 4 materialized views for Looker Studio dashboards

2. **`dag_create_mv_and_views_once`** (Manual/One-time DAG):
   - **create_materialized_views** - Creates BigQuery materialized views independently
   - Useful for initial setup or manual recreation of views
   - `schedule_interval=None` - only runs when manually triggered

3. **`dag_data_retention_cleanup`** (Daily scheduled DAG):
   - **cleanup_partitions** - Removes old Parquet partitions to manage disk space
   - Runs at 2 AM daily
   - Keeps 7 days of data with a 1-day buffer (deletes partitions older than 8 days)
   - Cleans up both fires and earthquakes datasets
   - Logs detailed metrics about deleted partitions and space freed

**BigQuery Export Process:**
- Reads tables from DuckDB: `fire_summary`, `earthquake_summary`, `combined_events`
- Exports to Parquet files locally
- Uploads Parquet to GCS bucket (`natural-events-staging`)
- Loads Parquet from GCS into BigQuery (`natural-events-pipeline.natural_events`)
- `combined_events` table uses partitioning by date and clustering by event type and severity
- Deletes temporary GCS files

**Materialized Views for Looker Studio:**
- `mv_combined_events` - World map with computed fields (colors, tooltips, bubble sizes)
- `mv_looker_fire_metrics` - Fire dashboard metrics with hourly auto-refresh
- `mv_looker_earthquake_metrics` - Earthquake metrics with hourly auto-refresh
- `v_looker_realtime_summary` - Real-time summary cards (24h/7d counts, alert status)

Enable the DAG in the Airflow UI to start scheduled hourly runs.

### Automation

Once everything has been setup and running for the first time, you can use the available script to instantly run docker, Nifi and Spark jobs
```
bash ./start_pipeline.sh
```

## Understanding the Data Flow

**Fires Data:**
- Updates every 15 minutes from NASA FIRMS
- CSV format with latitude, longitude, brightness, confidence, fire radiative power
- Partitioned by acquisition date in Parquet
- Spatial clustering applied (groups nearby fires into ~10km grid cells)

**Earthquakes Data:**
- Updates every minute from USGS
- GeoJSON format with magnitude, location, depth, time
- All earthquakes from the past hour

**Processing Pipeline:**

```
APIs → NiFi → Redpanda → Spark Streaming → Parquet → DuckDB + dbt → BigQuery
```

1. **Spark Streaming** (Near real-time processing)
   - Reads from Kafka topics in micro-batches (15-minute intervals)
   - Applies data cleaning and validation
   - Uses `from_csv` for NASA FIRMS data to handle embedded newlines
   - Adds `cluster_id` to fire data for spatial grouping (~24km grid cells)
   - Converts date strings to proper date types
   - Writes to Parquet with no compression for ARM64 compatibility
   - Fire data partitioned by `acq_date`, earthquakes by `event_date`
   - Checkpoints ensure fault tolerance and exactly-once semantics

2. **dbt Transformations** (Batch processing)
   - **Staging layer**: Creates clean views from raw Parquet files
   - **Marts layer**: Aggregates data into analysis-ready tables
   - Fire clusters are averaged by location and brightness for cleaner visualization
   - Runs via Airflow on an hourly schedule

3. **Airflow Orchestration** (Hourly)
   - Executes dbt models to transform data
   - Runs data quality tests
   - Exports DuckDB tables to BigQuery via GCS
   - Creates materialized views for Looker Studio
   - Maintains execution logs and metadata in PostgreSQL

4. **BigQuery Export**
   - `combined_events` table loaded with `WRITE_TRUNCATE` (snapshot model)
   - Other tables use `WRITE_APPEND` for incremental updates
   - Partitioning and clustering applied for better query performance
   - Materialized views auto-refresh hourly with computed dashboard fields

## Key Technical Details

**Streaming with NiFi and Redpanda:**
- NiFi pulls from NASA FIRMS (every 15 min) and USGS (every minute) APIs
- Redpanda acts as a Kafka-compatible message broker between NiFi and Spark
- Two topics: `fires-topic` and `earthquakes-topic` with 5MB message limits

**Spark Structured Streaming:**
- Processes data in 15-minute micro-batches for near real-time updates
- Uses `from_csv` with defined schema to handle messy CSV data from NASA
- Watermarks prevent unbounded state growth (1 hour for fires, 10 min for earthquakes)
- RocksDB state store for fault-tolerant checkpoints
- Single-worker cluster with 1GB memory (good enough for this scale)

**Fire Clustering:**
- Spark adds `cluster_id` using spatial grid: `FLOOR(lat/0.22)_FLOOR(lon/0.22)_date`
- Groups nearby fires within ~24km radius to avoid cluttered maps
- dbt aggregates clusters by averaging location and brightness
- Earthquakes stay individual for precise location tracking

**Data Retention Policy:**
- Automated cleanup runs daily at 2 AM
- Keeps 7 days of data plus a 1-day buffer (deletes anything older than 8 days)
- Logs metrics about deleted partitions and space freed
- Helps manage disk space without manual intervention

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

## Recent Updates

**Latest Changes:**
- Added data retention policy with automated daily cleanup (7-day retention + 1-day buffer)
- Updated Spark batch processing interval to 15 minutes for both fires and earthquakes
- Configured Spark cluster with 1GB worker memory for optimized performance
- Created dedicated DAG and script for partition cleanup to manage disk space

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