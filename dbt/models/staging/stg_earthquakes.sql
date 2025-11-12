{{ config(materialized='view') }}

SELECT
    magnitude,
    place,
    event_timestamp AS event_time,
    longitude,
    latitude,
    depth,
    event_type,
FROM read_parquet('/opt/spark-data/earthquakes/**/*.parquet')
WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL