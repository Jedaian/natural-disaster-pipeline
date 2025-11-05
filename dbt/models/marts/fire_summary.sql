{{ config(materialized='table') }}

SELECT 
    acq_timestamp AS date,
    satellite,
    confidence,
    COUNT(*) AS fire_count,
    AVG(bright_ti4) AS avg_bright_ti4,
    MAX(bright_ti4) AS max_bright_ti4,
    AVG(fire_radioactive_power) AS avg_frp,
    MAX(fire_radioactive_power) AS max_frp
FROM {{ ref('stg_fires') }}
GROUP BY acq_timestamp, satellite, confidence
ORDER BY acq_timestamp DESC