{{ config(materialized='view') }}

SELECT
  latitude,
  longitude,
  bright_ti4,
  event_timestamp AS event_time,
  satellite,
  confidence,
  frp AS fire_radioactive_power,
  daynight,
  event_type,
  cluster_id
FROM read_parquet('/opt/spark-data/fires/**/*.parquet')
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL