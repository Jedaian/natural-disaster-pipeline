{{ config(materialized='view') }}

SELECT
  latitude,
  longitude,
  acq_date,
  acq_time,
  satellite,
  confidence,
  frp AS fire_radioactive_power,
  daynight,
  event_type
FROM read_parquet('/usr/app/data/fires/**/*.parquet')
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL