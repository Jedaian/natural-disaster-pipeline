{{ config(materialized='view') }}

SELECT
    magnitude,
    place,
    CAST(time AS TIMESTAMP) AS event_time,
    longitude,
    latitude,
    depth,
    event_type,
    timestamp AS processed_at
FROM read_parquet('/usr/app/data/earthquakes/**/*.parquet')
WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL