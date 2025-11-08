# Natural Events Real-Time Data Pipeline

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.0-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-1.27.0-003366?style=for-the-badge&logo=apache-nifi&logoColor=white)](https://nifi.apache.org)
[![Apache Parquet](https://img.shields.io/badge/Apache%20Parquet-latest-50A0FF?style=for-the-badge&logo=apache&logoColor=white)](https://parquet.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.10.0-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Docker](https://img.shields.io/badge/Docker-latest-0db7ed?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1.0-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org)
[![Google BigQuery](https://img.shields.io/badge/BigQuery-latest-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Redpanda](https://img.shields.io/badge/Redpanda-24.2.7-FF3C00?style=for-the-badge&logo=redpanda&logoColor=white)](https://redpanda.com)

A streaming data pipeline that tracks natural disasters around the world in real-time. This project ingests fire hotspot data from NASA and earthquake data from USGS, processes it through a message broker, transforms it with Spark, and makes it queryable with DuckDB.

## What This Does

1. Pulls the latest fire and earthquake data from public APIs
2. Streams that data through Kafka (via Redpanda)
3. Processes it with Spark
4. Stores it in Parquet files
5. Orchestrates and transforms data with Airflow and dbt
6. Makes it ready for visualization

## Architecture

<image title="ETL Architecture" src="architecture/natural_events_architecture.png">

**Components:**

- **NiFi**: Data ingestion from APIs
- **Redpanda**: Kafka-compatible message broker
- **Spark**: Stream processing
- **Parquet**: Column-oriented storage format
- **Airflow**: Pipeline orchestration and scheduling
- **dbt**: Data transformation using SQL
- **DuckDB**: Analytical database for querying Parquet files

## Prerequisites

- Docker Desktop installed
- About 4-5 GB of free disk space
- NASA FIRMS API key (https://firms.modaps.eosdis.nasa.gov/api/)
- M1/M2 Mac, or adjust the Docker images for your architecture

## Project Structure

```
natural-disasters-pipeline/
├── docker-compose.yml          # All services defined here
├── Dockerfile                  # Custom container
├── spark-apps/
│   └── streaming_app.py       # Spark streaming job
├── spark-data/
│   ├── fires/                 # Fire data parquet files
│   ├── earthquakes/           # Earthquake data parquet files
│   └── natural_events.duckdb  # DuckDB database
├── airflow/
│   ├── dags/                  # Airflow DAGs
│   ├── logs/                  # Airflow logs
│   └── plugins/               # Airflow plugins
└── dbt/
    ├── models/                # dbt models
    │   ├── staging/           # Raw data views
    │   └── marts/             # Aggregated tables
    └── profiles.yml           # DuckDB connection config
```

## Getting Started

### 1. Clone and Setup

```bash
git clone <your-repo>
cd natural-disasters-pipeline
mkdir -p spark-apps spark-data dbt
```

### 2. Start the Services

```bash
docker-compose up -d
```

This starts:
- NiFi (https://localhost:8443)
- Redpanda Console (http://localhost:8090)
- Spark Master UI (http://localhost:8080)
- Airflow UI (http://localhost:8081)

Wait a couple minutes for everything to initialize.

### 3. Configure NiFi

Access NiFi at https://localhost:8443/nifi (username: `admin`, password: `adminadminadmin`)

**NASA FIRMS flow:**

1. Add `InvokeHTTP` processor:
   - HTTP Method: GET
   - Remote URL: `https://firms.modaps.eosdis.nasa.gov/api/area/csv/YOUR_API_KEY/VIIRS_SNPP_NRT/world/1`
   - Run Schedule: 15 min
   - Auto-terminate: Original, Failure, Retry, No Retry

2. Add `PublishKafka_2_6` processor:
   - Create `KafkaConnectionService` controller service
   - Bootstrap Servers: `redpanda:9092`
   - Topic Name: `fires-topic`
   - Auto-terminate: success, failure

3. Connect InvokeHTTP (Response) to PublishKafka

**USGS Earthquake flow:**

Same setup with:
- Remote URL: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson`
- Run Schedule: 1 min
- Topic Name: `earthquakes-topic`

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

Access Airflow UI at http://localhost:8081

The Airflow DAG orchestrates:
- dbt transformations on a scheduled basis
- Staging views (`stg_fires`, `stg_earthquakes`) that clean and filter raw Parquet data
- Summary tables with aggregations by date, satellite, and confidence level
- A combined events table with both fires and earthquakes
- Data quality checks

Enable the DAG in the Airflow UI to start scheduled runs.

### 7. Query the Data

Query the data using Python and DuckDB:

```python
import duckdb
con = duckdb.connect('/path/to/spark-data/natural_events.duckdb')

# Check tables
con.execute("SHOW TABLES").fetchall()

# Query fire summary
con.execute("SELECT * FROM fire_summary LIMIT 10").fetchdf()

# Query earthquake summary
con.execute("SELECT * FROM earthquake_summary LIMIT 10").fetchdf()

# Query combined events
con.execute("SELECT event_type, COUNT(*) FROM combined_events GROUP BY event_type").fetchall()
```

## Understanding the Data Flow

**Fires Data:**
- Updates every 15 minutes from NASA FIRMS
- CSV format with latitude, longitude, brightness, confidence, fire radiative power
- Partitioned by acquisition date in Parquet

**Earthquakes Data:**
- Updates every minute from USGS
- GeoJSON format with magnitude, location, depth, time
- All earthquakes from the past hour

**Processing:**
- Spark reads from Kafka topics in micro-batches (30-second intervals)
- Applies data cleaning and validation
- Uses `from_csv` for NASA FIRMS data to handle embedded newlines
- Converts date strings to proper date types
- Writes to Parquet with no compression for ARM64 compatibility
- Fire data partitioned by acquisition date
- Checkpoints ensure fault tolerance and exactly-once semantics
- Airflow orchestrates dbt to aggregate data for analysis

## Technical Highlights

**CSV Parsing:**
- Uses `from_csv` with defined schema for robust parsing
- Handles embedded newlines in NASA FIRMS data
- Filters out header rows and empty lines

**Data Partitioning:**
- Fire data partitioned by `acq_date` (acquisition date)
- Directory structure: `/fires/acq_date=2025-11-05/`
- Enables efficient time-based queries
- DuckDB can read specific partitions when filtering by date

**Stream Processing:**
- RocksDB state store for Spark streaming checkpoints
- Adaptive query execution for dynamic query optimization
- Watermarks prevent unbounded state growth (15 min for fires, 1 min for earthquakes)
- `foreachBatch` provides batch-level monitoring

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

## What's Next

1. Add Looker Studio for visualization dashboards
2. Enhance Airflow DAGs with more complex scheduling and data quality checks
3. Split large messages in NiFi using SplitText processor
4. Add deduplication for duplicate events
5. Deploy to cloud (GCP, AWS) with BigQuery integration
6. Add alerting for major events (magnitude > 6 earthquakes, large fires)
7. Historical analysis with long-term data storage

## Tech Stack Summary

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Airflow | 2.8.0 | Pipeline orchestration and scheduling |
| Apache NiFi | 1.27.0 | Data ingestion from APIs |
| Redpanda | 24.2.7 | Message broker (Kafka-compatible) |
| Apache Spark | 3.5.1 | Stream processing |
| dbt | 1.10.0 | Data transformation |
| DuckDB | 1.1.0 | Analytical database |
| Parquet | - | Storage format |
| Docker | - | Containerization |

## Resources

- [NASA FIRMS API Docs](https://firms.modaps.eosdis.nasa.gov/api/)
- [USGS Earthquake API Docs](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Redpanda Documentation](https://docs.redpanda.com/)

## License

MIT License. See the [LICENSE](LICENSE) file for details.