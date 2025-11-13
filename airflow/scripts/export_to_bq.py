from __future__ import annotations
import os
import time
import pathlib
import logging
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound, Conflict, GoogleAPIError

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/spark-data/natural_events.duckdb")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/config/gcp-credentials.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

GCS_BUCKET = os.getenv("GCS_BUCKET", "natural-events-staging")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "natural_events")
TABLES = ["fire_summary", "earthquake_summary", "combined_events"]

LOCAL_EXPORT_DIR = os.getenv("LOCAL_EXPORT_DIR", "/opt/spark-data/exports")
CONCURRENCY = int(os.getenv("EXPORT_CONCURRENCY", "3"))
MAX_RETRIES = 5
RETRY_DELAY = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("export_to_bigquery")

def retry(func):
    def wrapper(*args, **kwargs):
        delay = RETRY_DELAY
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except (GoogleAPIError, Exception) as e:
                if attempt == MAX_RETRIES:
                    logger.exception("Max retries reached for %s", func.__name__)
                    raise
                logger.warning("Retry %d/%d after error: %s", attempt, MAX_RETRIES, e)
                time.sleep(delay)
                delay *= 2
    return wrapper

def init_clients():
    bq = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()
    gcs = storage.Client()
    return bq, gcs

def ensure_bq_dataset(client: bigquery.Client, dataset_id: str):
    ds_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(ds_ref)
        logger.info("Dataset %s.%s exists", client.project, dataset_id)
    except NotFound:
        ds = bigquery.Dataset(ds_ref)
        ds.location = "US"
        try:
            client.create_dataset(ds)
            logger.info("Created dataset %s.%s", client.project, dataset_id)
        except Conflict:
            pass
    return ds_ref

@retry
def duckdb_to_parquet(conn: duckdb.DuckDBPyConnection, table: str, out_path: str):
    bq_client = bigquery.Client()
    table_id = f"{bq_client.project}.{BQ_DATASET}.{table}"

    timestamp_columns = {
        "fire_summary": "event_date",
        "earthquake_summary": "event_date",
        "combined_events": "event_time",
    }

    ts_col = timestamp_columns.get(table, "event_timestamp")

    try:
        latest_query = f"SELECT MAX({ts_col}) AS last_ts FROM `{table_id}`"
        result = list(bq_client.query(latest_query).result())
        last_ts = result[0].last_ts if result and result[0].last_ts is not None else None
    except Exception:
        last_ts = None

    if table == "combined_events":
        sql = f"COPY (SELECT * FROM {table}) TO '{out_path}' (FORMAT PARQUET);"
        logger.info("Full export for combined_events (derived snapshot)")
    elif last_ts:
        sql = f"""
            COPY (
                SELECT * FROM {table}
                WHERE {ts_col} > TIMESTAMP '{last_ts}'
            ) TO '{out_path}' (FORMAT PARQUET);
        """
        logger.info("Incremental export for %s since %s", table, last_ts)
    else:
        sql = f"COPY (SELECT * FROM {table}) TO '{out_path}' (FORMAT PARQUET);"
        logger.info("Full export for %s (no existing data found)", table)

    conn.execute(sql)
    logger.info("Exported %s → %s", table, out_path)
    return out_path


@retry
def upload_to_gcs(gcs_client: storage.Client, bucket_name: str, local_file: str, dest_blob: str):
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob)
    blob.chunk_size = 5 * 1024 * 1024  # 5MB
    blob.upload_from_filename(local_file)
    logger.info("Uploaded %s to gs://%s/%s", local_file, bucket_name, dest_blob)
    return f"gs://{bucket_name}/{dest_blob}"

@retry
def load_parquet_to_bq(bq_client: bigquery.Client, gs_uri: str, dataset: str, table: str):
    table_id = f"{bq_client.project}.{dataset}.{table}"
    if table == "combined_events":
        logger.info("Creating partitioned table for combined_events")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_id}` (
            latitude FLOAT64,
            longitude FLOAT64,
            event_type STRING,
            intensity_measure FLOAT64,
            event_time TIMESTAMP,
            event_date DATE,
            severity_level STRING
        )
        PARTITION BY event_date
        CLUSTER BY event_type, severity_level
        """
        
        try:
            bq_client.query(create_table_sql).result()
            logger.info("Ensured partitioned table exists for combined_events")
        except Conflict:
            logger.info("Partitioned table already exists")
        write_mode = "WRITE_TRUNCATE"
    else:
        write_mode = "WRITE_APPEND"
        logger.info("Using WRITE_APPEND for %s", table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_mode,
        autodetect=True,
    )
    job = bq_client.load_table_from_uri(gs_uri, table_id, job_config=job_config)
    job.result()

    dest = bq_client.get_table(table_id)
    logger.info("Loaded %d rows into %s", dest.num_rows, table_id)
    return dest.num_rows


@retry
def delete_from_gcs(gcs_client: storage.Client, bucket_name: str, blob_path: str):
    blob = gcs_client.bucket(bucket_name).blob(blob_path)
    blob.delete()
    logger.info("Deleted gs://%s/%s", bucket_name, blob_path)

def export_table(table: str, bq_client, gcs_client, duckdb_path: str, dataset: str) -> Dict[str, str]:
    start = time.time()
    status = {"table": table, "status": "FAILED", "rows": 0}
    tmp_dir = pathlib.Path(LOCAL_EXPORT_DIR)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = tmp_dir / f"{table}_{int(start)}.parquet"
    blob_path = f"duckdb_exports/{parquet_path.name}"

    conn = duckdb.connect(duckdb_path)
    try:
        duckdb_to_parquet(conn, table, str(parquet_path))
        gs_uri = upload_to_gcs(gcs_client, GCS_BUCKET, str(parquet_path), blob_path)
        rows = load_parquet_to_bq(bq_client, gs_uri, dataset, table)
        delete_from_gcs(gcs_client, GCS_BUCKET, blob_path)
        status.update({"status": "SUCCESS", "rows": rows})
    except Exception as e:
        logger.exception("Export failed for %s: %s", table, e)
    finally:
        conn.close()
        if parquet_path.exists():
            parquet_path.unlink(missing_ok=True)
    elapsed = round(time.time() - start, 2)
    logger.info("%s export finished: %s in %.2fs", table, status["status"], elapsed)
    return status

def main():
    bq_client, gcs_client = init_clients()
    ensure_bq_dataset(bq_client, BQ_DATASET)

    logger.info("Starting export from %s to BigQuery dataset %s", DUCKDB_PATH, BQ_DATASET)
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(export_table, t, bq_client, gcs_client, DUCKDB_PATH, BQ_DATASET) for t in TABLES]
        results = [f.result() for f in as_completed(futures)]

    logger.info("All exports complete:")
    for r in results:
        logger.info("  %s → %s (%d rows)", r["table"], r["status"], r["rows"])
    logger.info("✓ DuckDB → GCS → BigQuery export finished successfully.")

if __name__ == "__main__":
    main()