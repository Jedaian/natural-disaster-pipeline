{{ config(materialized='table') }}

WITH fires AS (
    SELECT 
        latitude,
        longitude,
        'fire' AS event_type,
        bright_ti4 AS intensity_measure,
        acq_timestamp AS event_timestamp
    FROM {{ ref('stg_fires') }}
),
earthquakes AS (
    SELECT 
        latitude,
        longitude,
        'earthquake' as event_type,
        magnitude AS intensity_measure,
        timestamp AS event_timestamp
    FROM {{ ref('stg_earthquakes') }}
)

SELECT * FROM fires
UNION ALL
SELECT * FROM earthquakes