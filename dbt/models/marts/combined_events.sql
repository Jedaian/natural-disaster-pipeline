{{ config(materialized='table') }}

WITH fires AS (
    SELECT 
        latitude,
        longitude,
        'fire' as event_type,
        acq_date as event_date,
        brightness as intensity_measure
    FROM {{ ref('stg_fires') }}
),
earthquakes AS (
    SELECT 
        latitude,
        longitude,
        'earthquake' as event_type,
        CAST(time AS DATE) as event_date,
        mag as intensity_measure
    FROM {{ ref('stg_earthquakes') }}
)

SELECT * FROM fires
UNION ALL
SELECT * FROM earthquakes