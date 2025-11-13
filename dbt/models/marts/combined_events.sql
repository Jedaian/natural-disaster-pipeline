{{ config(materialized='view') }}

WITH fire_clusters AS (
  SELECT
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude,
    'fire' AS event_type,
    AVG(bright_ti4) AS intensity_measure,
    MAX(event_time) AS event_time,
    CAST(MAX(event_time) AS DATE) AS event_date,
    CASE 
      WHEN AVG(bright_ti4) >= 400 THEN 'severe'
      ELSE 'moderate'
    END AS severity_level
  FROM {{ ref('stg_fires') }}
  GROUP BY cluster_id, CAST(event_time AS DATE)
),
earthquakes AS (
  SELECT
    latitude,
    longitude,
    'earthquake' AS event_type,
    magnitude AS intensity_measure,
    event_time,
    CAST(event_time AS DATE) AS event_date,
    CASE 
      WHEN magnitude >= 6 THEN 'severe'
      ELSE 'moderate'
    END AS severity_level
  FROM {{ ref('stg_earthquakes') }}
),
combined AS (
  SELECT * FROM fire_clusters
  UNION ALL
  SELECT * FROM earthquakes
)
SELECT DISTINCT * FROM combined