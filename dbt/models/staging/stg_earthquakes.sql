{{ config(materialized='view') }}

SELECT
    magnitude,
    place,
    timestamp,
    longitude,
    latitude,
    depth,
    event_type,
FROM read_parquet('/usr/app/data/earthquakes/**/*.parquet')
WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL