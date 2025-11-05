{{ config(materialized='table') }}

SELECT 
    EXTRACT(HOUR FROM timestamp) AS hour,
    COUNT(*) AS earthquake_count,
    AVG(magnitude) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude,
    MIN(magnitude) AS min_magnitude,
    AVG(depth) AS avg_depth,
    MAX(depth) AS max_depth,
FROM {{ ref('stg_earthquakes') }}
GROUP BY hour
ORDER BY hour DESC