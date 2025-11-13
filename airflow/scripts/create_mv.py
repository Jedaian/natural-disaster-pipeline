import os
import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("create_materialized_views")

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/config/gcp-credentials.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "natural_events")

def create_mv_world_map(client):
    """1. Main world map MV for Looker Studio"""
    logger.info("Creating mv_combined_events (world map)")
    
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
      latitude,
      longitude,
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
    
    try:
        job = client.query(mv_sql)
        job.result()
        logger.info("mv_combined_events created successfully")
        return True
    except GoogleAPIError as e:
        logger.error(f"Failed to create mv_combined_events: {e}")
        return False


def create_mv_fire_metrics(client):
    """2. Fire dashboard metrics MV"""
    logger.info("Creating mv_looker_fire_metrics")
    
    mv_sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS `{client.project}.{BQ_DATASET}.mv_looker_fire_metrics`
    OPTIONS(
      enable_refresh = true,
      refresh_interval_minutes = 60
    )
    AS
    SELECT
      event_date,
      satellite,
      confidence,
      fire_count,
      ROUND(avg_brightness, 1) as avg_brightness,
      ROUND(max_brightness, 1) as max_brightness,
      ROUND(avg_frp, 1) as avg_fire_power,
      ROUND(max_frp, 1) as max_fire_power,
      
      CASE 
        WHEN confidence = 'h' THEN 'High'
        WHEN confidence = 'n' THEN 'Nominal'
        WHEN confidence = 'l' THEN 'Low'
        ELSE 'Unknown'
      END as confidence_level,
      
      CASE 
        WHEN max_frp > 500 THEN 'Extreme'
        WHEN max_frp > 200 THEN 'Severe'
        WHEN max_frp > 50 THEN 'Moderate'
        ELSE 'Low'
      END as severity_level,
      
      EXTRACT(DAYOFWEEK FROM event_date) as day_of_week,
      FORMAT_DATE('%Y-%m', event_date) as year_month
      
    FROM `{client.project}.{BQ_DATASET}.fire_summary`
    """
    
    try:
        job = client.query(mv_sql)
        job.result()
        logger.info("mv_looker_fire_metrics created successfully")
        return True
    except GoogleAPIError as e:
        logger.error(f"Failed to create mv_looker_fire_metrics: {e}")
        return False


def create_mv_earthquake_metrics(client):
    """3. Earthquake dashboard metrics MV"""
    logger.info("Creating mv_looker_earthquake_metrics")
    
    mv_sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS `{client.project}.{BQ_DATASET}.mv_looker_earthquake_metrics`
    OPTIONS(
      enable_refresh = true,
      refresh_interval_minutes = 60
    )
    AS
    SELECT
      event_date,
      hour,
      earthquake_count,
      ROUND(avg_magnitude, 2) as avg_magnitude,
      ROUND(max_magnitude, 2) as max_magnitude,
      ROUND(min_magnitude, 2) as min_magnitude,
      ROUND(avg_depth, 1) as avg_depth_km,
      ROUND(max_depth, 1) as max_depth_km,
      
      CASE 
        WHEN max_magnitude >= 6.0 THEN 'Significant Activity'
        WHEN max_magnitude >= 4.5 THEN 'Moderate Activity'
        WHEN max_magnitude >= 3.0 THEN 'Light Activity'
        ELSE 'Minor Activity'
      END as activity_level,
      
      CASE 
        WHEN avg_depth <= 70 THEN 'Shallow'
        WHEN avg_depth <= 300 THEN 'Intermediate'
        ELSE 'Deep'
      END as depth_category
      
    FROM `{client.project}.{BQ_DATASET}.earthquake_summary`
    """
    
    try:
        job = client.query(mv_sql)
        job.result()
        logger.info("mv_looker_earthquake_metrics created successfully")
        return True
    except GoogleAPIError as e:
        logger.error(f"Failed to create mv_looker_earthquake_metrics: {e}")
        return False


def create_realtime_summary_view(client):
    """4. Real-time summary view for dashboard cards"""
    logger.info("Creating v_looker_realtime_summary")
    
    view_sql = f"""
    CREATE OR REPLACE VIEW `{client.project}.{BQ_DATASET}.v_looker_realtime_summary` AS
    WITH recent_events AS (
      SELECT 
        event_type,
        event_time,
        intensity_measure,
        intensity_category
      FROM `{client.project}.{BQ_DATASET}.mv_combined_events`
      WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    )
    SELECT
      COUNTIF(event_type = 'fire') as fires_24h,
      COUNTIF(event_type = 'earthquake') as earthquakes_24h,
      COUNTIF(event_type = 'fire' AND intensity_measure > 350) as severe_fires_24h,
      COUNTIF(event_type = 'earthquake' AND intensity_measure >= 5.0) as significant_earthquakes_24h,
      
      (SELECT COUNT(*) FROM `{client.project}.{BQ_DATASET}.mv_combined_events` 
       WHERE event_type = 'fire' AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) as fires_7d,
      (SELECT COUNT(*) FROM `{client.project}.{BQ_DATASET}.mv_combined_events` 
       WHERE event_type = 'earthquake' AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) as earthquakes_7d,
      
      MAX(CASE WHEN event_type = 'fire' THEN intensity_measure END) as max_fire_intensity_24h,
      MAX(CASE WHEN event_type = 'earthquake' THEN intensity_measure END) as max_earthquake_magnitude_24h,
      
      CASE 
        WHEN MAX(CASE WHEN event_type = 'earthquake' THEN intensity_measure END) >= 6.0 THEN 'CRITICAL'
        WHEN MAX(CASE WHEN event_type = 'fire' THEN intensity_measure END) > 400 THEN 'HIGH'
        WHEN COUNT(*) > 100 THEN 'ELEVATED'
        ELSE 'NORMAL'
      END as alert_status,
      
      CURRENT_TIMESTAMP() as last_updated
    FROM recent_events
    """
    
    try:
        job = client.query(view_sql)
        job.result()
        logger.info("v_looker_realtime_summary created successfully")
        return True
    except GoogleAPIError as e:
        logger.error(f"Failed to create v_looker_realtime_summary: {e}")
        return False


def main():
    client = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()
    logger.info(f"Starting materialized view creation for project: {client.project}")
    
    results = []
    
    results.append(("mv_combined_events", create_mv_world_map(client)))
    results.append(("mv_looker_fire_metrics", create_mv_fire_metrics(client)))
    results.append(("mv_looker_earthquake_metrics", create_mv_earthquake_metrics(client)))
    results.append(("v_looker_realtime_summary", create_realtime_summary_view(client)))
    
    logger.info("\n" + "="*50)
    logger.info("Materialized View Creation Summary:")
    for name, success in results:
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"{name}: {status}")
    
    success_count = sum(1 for _, s in results if s)
    logger.info(f"\n{success_count}/{len(results)} views created successfully")
    
    if success_count == len(results):
        logger.info("All materialized views created successfully!")
    else:
        logger.warning("Some materialized views failed to create")

if __name__ == "__main__":
    main()