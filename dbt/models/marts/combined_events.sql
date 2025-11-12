{{ config(materialized='table') }}

WITH combined AS (
  SELECT
    latitude,
    longitude,
    event_type,
    intensity_measure,
    event_time,
    CAST(event_time AS DATE) AS event_date,
    CASE 
      WHEN event_type = 'earthquake' AND intensity_measure >= 6 THEN 'severe'
      WHEN event_type = 'fire' AND intensity_measure >= 400 THEN 'severe'
      ELSE 'moderate'
    END AS severity_level
  FROM (
    SELECT
      latitude,
      longitude,
      'fire' AS event_type,
      bright_ti4 AS intensity_measure,
      event_time
    FROM {{ ref('stg_fires') }}
    UNION ALL
    SELECT
      latitude,
      longitude,
      'earthquake' AS event_type,
      magnitude AS intensity_measure,
      event_time
    FROM {{ ref('stg_earthquakes') }}
  )
)
SELECT * FROM combined