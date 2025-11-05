{{ config(materialized='table') }}

SELECT 
    DATE_TRUNC('hour', CAST(event_time AS TIMESTAMP)) AS hour,
    COUNT(*) AS earthquake_count,
    AVG(mag) AS avg_magnitude,
    MAX(mag) AS max_magnitude,
    MIN(mag) AS min_magnitude,
    AVG(depth) AS avg_depth,
    MAX(depth) AS max_depth
FROM {{ ref('stg_earthquakes') }}
GROUP BY hour
ORDER BY hour DESC