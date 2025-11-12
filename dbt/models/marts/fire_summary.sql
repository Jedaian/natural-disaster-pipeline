{{ config(materialized='table') }}

SELECT 
  CAST(event_time AS DATE) AS event_date,
  satellite,
  confidence,
  COUNT(*) AS fire_count,
  ROUND(AVG(bright_ti4), 2) AS avg_brightness,
  MAX(bright_ti4) AS max_brightness,
  ROUND(AVG(fire_radioactive_power), 2) AS avg_frp,
  MAX(fire_radioactive_power) AS max_frp,
  CASE 
    WHEN AVG(bright_ti4) >= 400 THEN 'severe'
    WHEN AVG(bright_ti4) >= 350 THEN 'moderate'
    ELSE 'low'
  END AS fire_intensity_category
FROM {{ ref('stg_fires') }}
GROUP BY event_date, satellite, confidence
ORDER BY event_date DESC