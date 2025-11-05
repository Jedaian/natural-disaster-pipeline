{{ config(materialized='table') }}

SELECT 
    acq_date AS date,
    satellite,
    confidence,
    COUNT(*) AS fire_count,
    AVG(brightness) AS avg_brightness,
    MAX(brightness) AS max_brightness,
    AVG(fire_radiative_power) AS avg_frp,
    MAX(fire_radiative_power) AS max_frp
FROM {{ ref('stg_fires') }}
GROUP BY acq_date, satellite, confidence
ORDER BY acq_date DESC