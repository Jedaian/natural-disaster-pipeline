# Natural Events Real-Time Data Pipeline

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-003366?style=for-the-badge&logo=apache-nifi&logoColor=white)](https://nifi.apache.org)
[![Apache Parquet](https://img.shields.io/badge/Apache%20Parquet-50A0FF?style=for-the-badge&logo=apache&logoColor=white)](https://parquet.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=flat-square&logo=apachespark&logoColor=black)](https://spark.apache.org)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org)
[![Redpanda](https://img.shields.io/badge/Redpanda-FF3C00?style=for-the-badge&logo=redpanda&logoColor=white)](https://redpanda.com)

A streaming data pipeline that tracks natural disasters around the world in real-time. This project ingests fire hotspot data from NASA and earthquake data from USGS, processes it through a message broker, transforms it with Spark, and makes it queryable with DuckDB.

## What This Does

Think of this as a mini version of how companies handle real-time data at scale. Every few minutes, it:

1. Pulls the latest fire and earthquake data from public APIs
2. Streams that data through Kafka (via Redpanda)
3. Processes it with Spark
4. Stores it in Parquet files
5. Transforms it with dbt for analysis
6. Makes it ready for visualization

The whole thing runs locally on Docker, which makes it perfect for learning how these tools work together without needing cloud infrastructure.

## Architecture

Here's how data flows through the system:
<image title="ETL Architecture" src="architecture/natural_events_architecture.png">

**Why these tools?**

- **NiFi**: Visual interface for pulling data from APIs. You can see exactly what's happening at each step.
- **Redpanda**: Kafka-compatible message broker that's lighter and easier to run locally than Kafka itself.
- **Spark**: Handles stream processing. Bit of overkill for this data volume, but great for learning.
- **Parquet**: Column-oriented format that's perfect for analytical queries.
- **dbt**: Transforms raw data into clean, aggregated tables using SQL.
- **DuckDB**: In-process database that can query Parquet files directly. Like SQLite but for analytics.

## Prerequisites

You need:
- Docker Desktop installed
- About 4-5 GB of free disk space
- A NASA FIRMS API key (free, instant approval at https://firms.modaps.eosdis.nasa.gov/api/)
- An M1/M2 Mac, or adjust the Docker images for your architecture

## Project Structure

```
natural-disasters-pipeline/
├── docker-compose.yml          # All services defined here
├── Dockerfile                  # Custom dbt container
├── spark-apps/
│   └── streaming_app.py       # Spark streaming job
├── spark-data/
│   ├── fires/                 # Fire data parquet files
│   ├── earthquakes/           # Earthquake data parquet files
│   └── natural_events.duckdb  # DuckDB database
└── dbt/
    └── models/
    │   ├── staging/       # Raw data views
    │   └── marts/         # Aggregated tables
    └── profiles.yml       # DuckDB connection config
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
- dbt container (runs in background)

Wait a couple minutes for everything to initialize.

### 3. Configure NiFi

Go to https://localhost:8443/nifi (username: `admin`, password: `adminadminadmin`)

**Create the NASA FIRMS flow:**

1. Add an `InvokeHTTP` processor:
   - HTTP Method: GET
   - Remote URL: `https://firms.modaps.eosdis.nasa.gov/api/area/csv/YOUR_API_KEY/VIIRS_SNPP_NRT/world/1`
   - Run Schedule: 15 min
   - Auto-terminate: Original, Failure, Retry, No Retry

2. Add a `PublishKafka_2_6` processor:
   - Create a `KafkaConnectionService` controller service
   - Bootstrap Servers: `redpanda:9092`
   - Topic Name: `fires-topic`
   - Auto-terminate: success, failure

3. Connect InvokeHTTP (Response) to PublishKafka

**Create the USGS Earthquake flow:**

Same setup but use:
- Remote URL: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson`
- Run Schedule: 1 min
- Topic Name: `earthquakes-topic`

Start all processors and verify data flows through.

### 4. Create Redpanda Topics

```bash
docker exec -it redpanda rpk topic create fires-topic -c max.message.bytes=5242880
docker exec -it redpanda rpk topic create earthquakes-topic -c max.message.bytes=5242880
```

The larger message size is needed because NASA returns a lot of fire data.

### 5. Run Spark Streaming

The `streaming_app.py` file should already be in `spark-apps/`. Submit it to Spark:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /opt/spark-apps/streaming_app.py
```

This will:
- Consume messages from both Redpanda topics
- Parse CSV (fires) and JSON (earthquakes) with robust data cleaning
- Handle special characters and newlines in NASA FIRMS data using `from_csv`
- Write to Parquet files every 30 seconds with no compression (ARM64 compatible)
- Partition fire data by acquisition date for efficient querying
- Monitor and display batch statistics (record count, sample data)
- Use watermarks to handle late-arriving data (15 min for fires, 1 min for earthquakes)
- Maintain checkpoints for fault tolerance
- Run continuously until you stop it (Ctrl+C)

You should see log output showing batches being processed with record counts and sample data.

### 6. Transform Data with dbt

Once Spark has written some data, run dbt transformations:

```bash
docker exec -it dbt bash
cd /usr/app/dbt/natural_events
dbt run
```

This creates:
- Staging views (`stg_fires`, `stg_earthquakes`) that clean and filter the raw Parquet data
- Summary tables with aggregations by date, satellite, and confidence level
- A combined events table with both fires and earthquakes for unified analysis
- All models use optimized SQL with proper formatting for maintainability

### 7. Query the Data

You can query the data using Python and DuckDB:

```bash
docker exec -it dbt python3
```

```python
import duckdb
con = duckdb.connect('/usr/app/data/natural_events.duckdb')

# Check what tables exist
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
- CSV format with columns: latitude, longitude, brightness, confidence, fire radiative power, etc.
- Partitioned by acquisition date in Parquet
- Useful for tracking wildfire activity globally

**Earthquakes Data:**
- Updates every minute from USGS
- GeoJSON format with magnitude, location, depth, time
- All earthquakes from the past hour
- Most events are small (magnitude < 3)

**Processing:**
- Spark reads from Kafka topics in micro-batches (30-second intervals)
- Applies robust data cleaning (regex to remove special characters, trim whitespace)
- Uses `from_csv` for NASA FIRMS data to handle embedded newlines correctly
- Filters out header rows and empty lines
- Converts date strings to proper date types
- Writes to Parquet with no compression for ARM64 compatibility
- Fire data partitioned by acquisition date for efficient querying
- Checkpoints ensure fault tolerance and exactly-once semantics
- dbt then aggregates this data for analysis

## Technical Highlights

**Advanced CSV Parsing for NASA FIRMS Data:**
The NASA FIRMS API returns CSV data that contains embedded newlines within the data itself, which breaks standard CSV parsing. The Spark script handles this by:
- Using regex to clean quotes and carriage returns from the raw data
- Splitting on newlines and exploding into individual lines
- Using `from_csv` with the defined schema instead of basic string splitting
- Filtering out header rows and empty lines
- This approach is more robust than standard CSV readers for messy real-world data

**Data Partitioning Strategy:**
- Fire data is partitioned by `acq_date` (acquisition date) in Parquet format
- This creates a directory structure like `/fires/acq_date=2025-11-05/`
- Enables efficient time-based queries without scanning all data
- dbt and DuckDB can read specific partitions when filtering by date

**Stream Processing Optimizations:**
- RocksDB state store for Spark streaming checkpoints (better performance than default)
- Adaptive query execution enabled for dynamic query optimization
- Watermarks prevent unbounded state growth from late-arriving data (15 min for fires, 1 min for earthquakes)
- `foreachBatch` provides batch-level monitoring and control
- Batch info printed with record count and sample data for visibility

## Common Issues

**NiFi can't publish to Redpanda:**
- Check the KafkaConnectionService uses `redpanda:9092`
- Verify both containers are on `pipeline-network`
- Message size might be too large (increase with rpk topic alter-config)

**Spark job fails with Snappy compression error:**
- This is an M1/M2 Mac (ARM64) issue with native compression libraries
- The code already has `option("compression", "none")` to work around it
- No compression slightly increases file size but ensures compatibility

**No data in Parquet files:**
- Check if NiFi is actually fetching data (view the queue)
- Check if Redpanda topics have messages (Redpanda Console)
- Check Spark logs for errors

**dbt can't find tables:**
- Make sure Spark has written Parquet files to spark-data/
- Check the paths in dbt models match your actual file locations

## What's Next

This is a learning project, so here are some ideas to extend it:

1. **Add Looker Studio** for visualization dashboards
2. **Add Airflow** to orchestrate the pipeline on a schedule instead of running continuously
3. **Split large messages in NiFi** using SplitText processor before publishing to Kafka
4. **Add deduplication** to handle cases where the same event appears in multiple fetches
5. **Deploy to cloud** (GCP, AWS) instead of running locally
6. **Add alerting** for major events (magnitude > 6 earthquakes, large fires)
7. **Historical analysis** by storing data long-term and analyzing trends

## Tech Stack Summary

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache NiFi | latest | Data ingestion from APIs |
| Redpanda | latest | Message broker (Kafka-compatible) |
| Apache Spark | 3.4.0 | Stream processing |
| dbt | 1.10.0 | Data transformation |
| DuckDB | latest | Analytical database |
| Parquet | - | Storage format |
| Docker | - | Containerization |

## Resources

- [NASA FIRMS API Docs](https://firms.modaps.eosdis.nasa.gov/api/)
- [USGS Earthquake API Docs](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
- [dbt Documentation](https://docs.getdbt.com/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Redpanda Documentation](https://docs.redpanda.com/)

## License

MIT License. See the [LICENSE](LICENSE) file for details.

## Notes

This project was built to learn the fundamentals of real-time data engineering. The tools here (especially Spark) are overkill for the actual data volumes, but that's intentional. In production, you'd use simpler solutions for this scale, but understanding how these tools work together is valuable for when you do need them at scale.

The data sources are public APIs, so be respectful with request rates. The current setup (15 min for fires, 1 min for earthquakes) is reasonable and won't hit any rate limits.