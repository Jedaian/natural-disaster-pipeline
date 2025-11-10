{{ config(materialized='view') }}

SELECT
  latitude,
  longitude,
  bright_ti4,
  timestamp AS acq_timestamp,
  satellite,
  confidence,
  frp AS fire_radioactive_power,
  daynight,
  event_type
FROM read_parquet('/opt/spark-data/fires/**/*.parquet')
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL