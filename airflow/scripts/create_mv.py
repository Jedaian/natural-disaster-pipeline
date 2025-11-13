import os
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("create_materialized_view")

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/config/gcp-credentials.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "natural_events")

def create_materialized_view():
    client = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()
    
    mv_sql = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{client.project}.{BQ_DATASET}.mv_combined_events`
    PARTITION BY event_date
    CLUSTER BY event_type, severity_level
    AS
    SELECT
      event_type,
      intensity_measure,
      event_time,
      event_date,
      severity_level,
      CONCAT(CAST(latitude AS STRING), ",", CAST(longitude AS STRING)) AS latlong,
      CASE 
        WHEN event_type = 'fire' THEN
          CASE 
            WHEN intensity_measure > 400 THEN 'Extreme Fire'
            WHEN intensity_measure > 350 THEN 'Severe Fire'
            WHEN intensity_measure > 320 THEN 'Moderate Fire'
            ELSE 'Low Fire'
          END
        WHEN event_type = 'earthquake' THEN
          CASE 
            WHEN intensity_measure >= 7.0 THEN 'Major Earthquake'
            WHEN intensity_measure >= 5.5 THEN 'Strong Earthquake'
            WHEN intensity_measure >= 4.0 THEN 'Moderate Earthquake'
            WHEN intensity_measure >= 2.5 THEN 'Light Earthquake'
            ELSE 'Minor Earthquake'
          END
      END AS intensity_category,
      CASE 
        WHEN event_type = 'fire' THEN
          CASE 
            WHEN intensity_measure > 400 THEN '#8B0000'
            WHEN intensity_measure > 350 THEN '#FF0000'
            WHEN intensity_measure > 320 THEN '#FF6600'
            ELSE '#FFA500'
          END
        WHEN event_type = 'earthquake' THEN
          CASE 
            WHEN intensity_measure >= 6.0 THEN '#4B0082'
            WHEN intensity_measure >= 5.0 THEN '#0000FF'
            WHEN intensity_measure >= 4.0 THEN '#1E90FF'
            WHEN intensity_measure >= 3.0 THEN '#87CEEB'
            ELSE '#ADD8E6'
          END
      END AS map_color,
      CASE 
        WHEN event_type = 'fire' THEN
          GREATEST(1, LEAST(100, (intensity_measure - 300) / 2))
        WHEN event_type = 'earthquake' THEN
          POWER(2, intensity_measure)
      END AS bubble_size,
      CONCAT(
        CASE 
          WHEN event_type = 'fire' THEN CONCAT('Fire Intensity: ', CAST(ROUND(intensity_measure, 1) AS STRING), ' K')
          WHEN event_type = 'earthquake' THEN CONCAT('Magnitude: ', CAST(ROUND(intensity_measure, 1) AS STRING))
        END,
        ' | Time: ', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M UTC', event_time)
      ) AS tooltip_text
    FROM `{client.project}.{BQ_DATASET}.combined_events`
    """
    
    logger.info("Creating materialized view mv_combined_events...")
    job = client.query(mv_sql)
    job.result()
    logger.info("✓ Materialized view created successfully")

if __name__ == "__main__":
    create_materialized_view()