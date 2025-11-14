import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def cleanup_dataset(dataset_name, partition_key, retention_days, base_path, dry_run, buffer_days):
    cutoff_date = datetime.now() - timedelta(days=retention_days + buffer_days)
    
    dataset_path = Path(base_path) / dataset_name
    
    metrics = {
        'dataset': dataset_name,
        'partitions_scanned': 0,
        'partitions_deleted': 0,
        'bytes_freed': 0,
        'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
        'deleted_partitions': [],
        'errors': []
    }
    
    logger.info(f"Starting cleanup for {dataset_name}")
    logger.info(f"  Base path: {base_path}")
    logger.info(f"  Retention: {retention_days} days (+ {buffer_days} day buffer)")
    logger.info(f"  Cutoff date: {metrics['cutoff_date']}")
    logger.info(f"  Dry run: {dry_run}")
    
    if not dataset_path.exists():
        error_msg = f"Dataset path not found: {dataset_path}"
        logger.error(error_msg)
        metrics['errors'].append(error_msg)
        return metrics
    
    for partition_dir in sorted(dataset_path.iterdir()):
        if not partition_dir.is_dir():
            continue
            
        metrics['partitions_scanned'] += 1
        
        try:
            if '=' not in partition_dir.name:
                logger.debug(f"Skipping non-partition directory: {partition_dir.name}")
                continue
                
            date_str = partition_dir.name.split('=')[1]
            partition_date = datetime.strptime(date_str, '%Y-%m-%d')
            
            if partition_date < cutoff_date:
                partition_size = sum(
                    f.stat().st_size for f in partition_dir.rglob('*') if f.is_file()
                )
                
                size_mb = partition_size / (1024**2)
                
                if dry_run:
                    logger.info(f"[DRY RUN] Would delete: {partition_dir.name} ({size_mb:.2f} MB)")
                else:
                    logger.info(f"Deleting partition: {partition_dir.name} ({size_mb:.2f} MB)")
                    shutil.rmtree(partition_dir)
                
                metrics['partitions_deleted'] += 1
                metrics['bytes_freed'] += partition_size
                metrics['deleted_partitions'].append({
                    'name': partition_dir.name,
                    'date': date_str,
                    'size_mb': round(size_mb, 2)
                })
            else:
                logger.debug(f"Keeping partition: {partition_dir.name} (newer than cutoff)")
                
        except ValueError as e:
            error_msg = f"Failed to parse date from {partition_dir.name}: {e}"
            logger.warning(error_msg)
            metrics['errors'].append(error_msg)
        except Exception as e:
            error_msg = f"Error processing {partition_dir.name}: {e}"
            logger.error(error_msg)
            metrics['errors'].append(error_msg)
    
    logger.info(f"Cleanup summary for {dataset_name}:")
    logger.info(f"  Partitions scanned: {metrics['partitions_scanned']}")
    logger.info(f"  Partitions deleted: {metrics['partitions_deleted']}")
    logger.info(f"  Space freed: {metrics['bytes_freed'] / (1024**2):.2f} MB")
    logger.info(f"  Mode: {'DRY RUN' if dry_run else 'ACTUAL DELETION'}")
    
    if metrics['errors']:
        logger.warning(f"  Errors encountered: {len(metrics['errors'])}")
        for error in metrics['errors']:
            logger.warning(f"    - {error}")
    
    logger.info("")
    
    return metrics


def main():
    logger.info("=" * 70)
    logger.info("DATA RETENTION CLEANUP - STARTING")
    logger.info("=" * 70)
    logger.info(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    fires_metrics = cleanup_dataset(
        dataset_name='fires',
        partition_key='acq_date',
        retention_days=7,
        base_path='/opt/spark-data',
        dry_run=False,
        buffer_days=1
    )
    
    earthquakes_metrics = cleanup_dataset(
        dataset_name='earthquakes',
        partition_key='event_date',
        retention_days=7,
        base_path='/opt/spark-data',
        dry_run=False,
        buffer_days=1
    )
    
    total_partitions = fires_metrics['partitions_deleted'] + earthquakes_metrics['partitions_deleted']
    total_bytes = fires_metrics['bytes_freed'] + earthquakes_metrics['bytes_freed']
    total_mb = total_bytes / (1024**2)
    total_gb = total_bytes / (1024**3)
    total_errors = len(fires_metrics['errors']) + len(earthquakes_metrics['errors'])
    
    # Final report
    logger.info("=" * 70)
    logger.info("DATA RETENTION CLEANUP - FINAL REPORT")
    logger.info("=" * 70)
    logger.info("")
    logger.info("TOTALS:")
    logger.info(f"  Total partitions scanned: {fires_metrics['partitions_scanned'] + earthquakes_metrics['partitions_scanned']}")
    logger.info(f"  Total partitions deleted: {total_partitions}")
    logger.info(f"  Total space freed: {total_mb:.2f} MB ({total_gb:.3f} GB)")
    logger.info(f"  Total errors: {total_errors}")
    logger.info("")
    logger.info("BY DATASET:")
    logger.info(f"  Fires: {fires_metrics['partitions_deleted']} partitions, {fires_metrics['bytes_freed'] / (1024**2):.2f} MB")
    logger.info(f"  Earthquakes: {earthquakes_metrics['partitions_deleted']} partitions, {earthquakes_metrics['bytes_freed'] / (1024**2):.2f} MB")
    
    if total_errors > 0:
        logger.warning("")
        logger.warning("ERRORS ENCOUNTERED:")
        if fires_metrics['errors']:
            logger.warning("  Fires dataset:")
            for error in fires_metrics['errors']:
                logger.warning(f"    - {error}")
        if earthquakes_metrics['errors']:
            logger.warning("  Earthquakes dataset:")
            for error in earthquakes_metrics['errors']:
                logger.warning(f"    - {error}")
    
    logger.info("=" * 70)
    logger.info("DATA RETENTION CLEANUP - COMPLETED")
    logger.info("=" * 70)
    
    if total_errors > 0:
        return 1
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)