{{ config(materialized='table') }}

SELECT 
  CAST(event_time AS DATE) AS event_date,
  EXTRACT(HOUR FROM event_time) AS hour,
  COUNT(*) AS earthquake_count,
  ROUND(AVG(magnitude), 2) AS avg_magnitude,
  MAX(magnitude) AS max_magnitude,
  MIN(magnitude) AS min_magnitude,
  ROUND(AVG(depth), 2) AS avg_depth,
  MAX(depth) AS max_depth
FROM {{ ref('stg_earthquakes') }}
GROUP BY event_date, hour
ORDER BY event_date DESC, hour DESC