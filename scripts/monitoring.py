"""
Monitoring & Observability Module
Tracks pipeline health, performance metrics, and data quality
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineMonitor:
    """Monitors pipeline execution and data quality metrics"""
    
    def __init__(self):
        self.mysql_engine = create_engine(
            db_config.mysql_connection_string,
            pool_pre_ping=True
        )
        self.postgres_engine = create_engine(
            db_config.postgres_connection_string,
            pool_pre_ping=True
        )
        logger.info("Pipeline Monitor initialized")
    
    def get_pipeline_health_status(self) -> Dict:
        """
        Get overall pipeline health status
        
        Returns:
            dict: Health metrics and status
        """
        try:
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'overall_status': 'HEALTHY',
                'components': {}
            }
            
            # Check MySQL staging database
            mysql_status = self._check_mysql_health()
            health_status['components']['mysql_staging'] = mysql_status
            
            # Check PostgreSQL analytics database
            postgres_status = self._check_postgres_health()
            health_status['components']['postgres_analytics'] = postgres_status
            
            # Check data freshness
            freshness = self._check_data_freshness()
            health_status['components']['data_freshness'] = freshness
            
            # Check validation status
            validation = self._check_validation_status()
            health_status['components']['data_quality'] = validation
            
            # Determine overall status
            if any(comp['status'] == 'UNHEALTHY' for comp in health_status['components'].values()):
                health_status['overall_status'] = 'UNHEALTHY'
            elif any(comp['status'] == 'WARNING' for comp in health_status['components'].values()):
                health_status['overall_status'] = 'WARNING'
            
            logger.info(f"Health check complete: {health_status['overall_status']}")
            return health_status
            
        except Exception as e:
            logger.error(f"Error checking pipeline health: {str(e)}")
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_status': 'ERROR',
                'error': str(e)
            }
    
    def _check_mysql_health(self) -> Dict:
        """Check MySQL database health"""
        try:
            with self.mysql_engine.connect() as conn:
                # Check connection
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
                
                # Get record count
                result = conn.execute(text("SELECT COUNT(*) FROM staging_flights"))
                record_count = result.fetchone()[0]
                
                return {
                    'status': 'HEALTHY',
                    'connection': 'OK',
                    'record_count': record_count,
                    'message': f'{record_count:,} records in staging'
                }
        except Exception as e:
            logger.error(f"MySQL health check failed: {str(e)}")
            return {
                'status': 'UNHEALTHY',
                'connection': 'FAILED',
                'error': str(e)
            }
    
    def _check_postgres_health(self) -> Dict:
        """Check PostgreSQL database health"""
        try:
            with self.postgres_engine.connect() as conn:
                # Check connection
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
                
                # Get record count
                result = conn.execute(text("SELECT COUNT(*) FROM flights_analytics"))
                record_count = result.fetchone()[0]
                
                # Check KPI tables
                kpi_tables = [
                    'kpi_average_fare_by_airline',
                    'kpi_seasonal_fare_variation',
                    'kpi_booking_count_by_airline',
                    'kpi_popular_routes'
                ]
                
                kpi_status = {}
                for table in kpi_tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    kpi_status[table] = result.fetchone()[0]
                
                return {
                    'status': 'HEALTHY',
                    'connection': 'OK',
                    'analytics_records': record_count,
                    'kpi_tables': kpi_status,
                    'message': f'{record_count:,} analytics records, {len(kpi_status)} KPI tables'
                }
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {str(e)}")
            return {
                'status': 'UNHEALTHY',
                'connection': 'FAILED',
                'error': str(e)
            }
    
    def _check_data_freshness(self) -> Dict:
        """Check if data is fresh (recently updated)"""
        try:
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT MAX(created_at) as last_update FROM staging_flights"
                ))
                last_update = result.fetchone()[0]
                
                if last_update:
                    age_hours = (datetime.now() - last_update).total_seconds() / 3600
                    
                    if age_hours < 24:
                        status = 'HEALTHY'
                        message = f'Data updated {age_hours:.1f} hours ago'
                    elif age_hours < 48:
                        status = 'WARNING'
                        message = f'Data is {age_hours:.1f} hours old (>24h)'
                    else:
                        status = 'UNHEALTHY'
                        message = f'Data is stale ({age_hours:.1f} hours old)'
                    
                    return {
                        'status': status,
                        'last_update': last_update.isoformat(),
                        'age_hours': round(age_hours, 2),
                        'message': message
                    }
                else:
                    return {
                        'status': 'UNHEALTHY',
                        'message': 'No data in staging table'
                    }
        except Exception as e:
            logger.error(f"Data freshness check failed: {str(e)}")
            return {
                'status': 'ERROR',
                'error': str(e)
            }
    
    def _check_validation_status(self) -> Dict:
        """Check recent data validation results"""
        try:
            with self.mysql_engine.connect() as conn:
                # Get latest validation results
                result = conn.execute(text("""
                    SELECT 
                        check_name,
                        check_status,
                        check_timestamp
                    FROM data_quality_log
                    WHERE check_timestamp > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                    ORDER BY check_timestamp DESC
                    LIMIT 10
                """))
                
                validations = result.fetchall()
                
                if not validations:
                    return {
                        'status': 'WARNING',
                        'message': 'No recent validation checks (last 24h)'
                    }
                
                failed_checks = [v for v in validations if v[1] == 'FAILED']
                
                if failed_checks:
                    return {
                        'status': 'UNHEALTHY',
                        'failed_checks': len(failed_checks),
                        'message': f'{len(failed_checks)} validation check(s) failed'
                    }
                else:
                    return {
                        'status': 'HEALTHY',
                        'total_checks': len(validations),
                        'message': f'{len(validations)} validation checks passed'
                    }
        except Exception as e:
            logger.error(f"Validation status check failed: {str(e)}")
            return {
                'status': 'ERROR',
                'error': str(e)
            }
    
    def get_performance_metrics(self) -> Dict:
        """
        Get pipeline performance metrics
        
        Returns:
            dict: Performance statistics
        """
        try:
            with self.postgres_engine.connect() as conn:
                # Get execution metrics from last 7 days
                result = conn.execute(text("""
                    SELECT 
                        task_id,
                        COUNT(*) as execution_count,
                        AVG(EXTRACT(EPOCH FROM execution_time)) as avg_time,
                        MIN(EXTRACT(EPOCH FROM execution_time)) as min_time,
                        MAX(EXTRACT(EPOCH FROM execution_time)) as max_time,
                        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failure_count
                    FROM pipeline_execution_log
                    WHERE execution_date > NOW() - INTERVAL '7 days'
                    GROUP BY task_id
                    ORDER BY task_id
                """))
                
                metrics = []
                for row in result:
                    metrics.append({
                        'task_id': row[0],
                        'execution_count': row[1],
                        'avg_execution_time': round(row[2], 2) if row[2] else 0,
                        'min_execution_time': round(row[3], 2) if row[3] else 0,
                        'max_execution_time': round(row[4], 2) if row[4] else 0,
                        'success_count': row[5],
                        'failure_count': row[6],
                        'success_rate': round((row[5] / row[1] * 100), 2) if row[1] > 0 else 0
                    })
                
                return {
                    'timestamp': datetime.now().isoformat(),
                    'period': 'Last 7 days',
                    'tasks': metrics
                }
        except Exception as e:
            logger.error(f"Error getting performance metrics: {str(e)}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def get_data_quality_metrics(self) -> Dict:
        """
        Get data quality metrics
        
        Returns:
            dict: Data quality statistics
        """
        try:
            quality_metrics = {}
            
            # Check for null values in critical fields
            with self.postgres_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(date_of_journey) as records_with_dates,
                        COUNT(*) - COUNT(date_of_journey) as missing_dates,
                        COUNT(airline) as records_with_airline,
                        COUNT(total_fare) as records_with_fare,
                        AVG(total_fare) as avg_fare,
                        MIN(total_fare) as min_fare,
                        MAX(total_fare) as max_fare
                    FROM flights_analytics
                """))
                
                row = result.fetchone()
                quality_metrics['completeness'] = {
                    'total_records': row[0],
                    'date_coverage': round((row[1] / row[0] * 100), 2) if row[0] > 0 else 0,
                    'missing_dates': row[2],
                    'airline_coverage': round((row[3] / row[0] * 100), 2) if row[0] > 0 else 0,
                    'fare_coverage': round((row[4] / row[0] * 100), 2) if row[0] > 0 else 0
                }
                
                quality_metrics['fare_statistics'] = {
                    'average': round(row[5], 2) if row[5] else 0,
                    'minimum': round(row[6], 2) if row[6] else 0,
                    'maximum': round(row[7], 2) if row[7] else 0
                }
            
            # Get validation history
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        check_name,
                        SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) as passed,
                        SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                        COUNT(*) as total
                    FROM data_quality_log
                    WHERE check_timestamp > DATE_SUB(NOW(), INTERVAL 7 DAY)
                    GROUP BY check_name
                """))
                
                validation_history = []
                for row in result:
                    validation_history.append({
                        'check_name': row[0],
                        'passed': row[1],
                        'failed': row[2],
                        'total': row[3],
                        'pass_rate': round((row[1] / row[3] * 100), 2) if row[3] > 0 else 0
                    })
                
                quality_metrics['validation_history'] = validation_history
            
            return {
                'timestamp': datetime.now().isoformat(),
                'metrics': quality_metrics
            }
        except Exception as e:
            logger.error(f"Error getting data quality metrics: {str(e)}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def detect_anomalies(self) -> Dict:
        """
        Detect anomalies in data
        
        Returns:
            dict: Anomaly detection results
        """
        try:
            anomalies = []
            
            with self.postgres_engine.connect() as conn:
                # Check for unusual fare values
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as count,
                        AVG(total_fare) as avg_fare,
                        STDDEV(total_fare) as stddev_fare
                    FROM flights_analytics
                """))
                
                row = result.fetchone()
                avg_fare = row[1]
                stddev_fare = row[2]
                
                if avg_fare and stddev_fare:
                    # Find outliers (> 3 standard deviations)
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) 
                        FROM flights_analytics
                        WHERE total_fare > {avg_fare + (3 * stddev_fare)}
                           OR total_fare < {avg_fare - (3 * stddev_fare)}
                    """))
                    
                    outlier_count = result.fetchone()[0]
                    
                    if outlier_count > 0:
                        anomalies.append({
                            'type': 'FARE_OUTLIERS',
                            'severity': 'INFO',
                            'count': outlier_count,
                            'message': f'{outlier_count} records with unusual fare values detected'
                        })
                
                # Check for duplicate records
                result = conn.execute(text("""
                    SELECT airline, source, destination, departure_time, COUNT(*)
                    FROM flights_analytics
                    GROUP BY airline, source, destination, departure_time
                    HAVING COUNT(*) > 1
                """))
                
                duplicates = result.fetchall()
                if duplicates:
                    anomalies.append({
                        'type': 'DUPLICATES',
                        'severity': 'WARNING',
                        'count': len(duplicates),
                        'message': f'{len(duplicates)} potential duplicate record groups found'
                    })
                
                # Check for missing seasonal classification
                result = conn.execute(text("""
                    SELECT COUNT(*)
                    FROM flights_analytics
                    WHERE season = 'Unknown' OR season IS NULL
                """))
                
                unknown_season = result.fetchone()[0]
                if unknown_season > 0:
                    anomalies.append({
                        'type': 'MISSING_SEASON',
                        'severity': 'WARNING',
                        'count': unknown_season,
                        'message': f'{unknown_season} records with unknown season classification'
                    })
            
            return {
                'timestamp': datetime.now().isoformat(),
                'anomalies_detected': len(anomalies),
                'anomalies': anomalies
            }
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def generate_health_report(self) -> str:
        """
        Generate comprehensive health report
        
        Returns:
            str: Formatted health report
        """
        try:
            health = self.get_pipeline_health_status()
            performance = self.get_performance_metrics()
            quality = self.get_data_quality_metrics()
            anomalies = self.detect_anomalies()
            
            report = []
            report.append("=" * 80)
            report.append("FLIGHT PRICE PIPELINE - HEALTH REPORT")
            report.append("=" * 80)
            report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report.append("")
            
            # Overall Status
            report.append(f"Overall Status: {health['overall_status']}")
            report.append("-" * 80)
            
            # Component Health
            report.append("\nCOMPONENT HEALTH:")
            for component, status in health['components'].items():
                report.append(f"  {component}: {status['status']}")
                if 'message' in status:
                    report.append(f"    → {status['message']}")
            
            # Performance Metrics
            report.append("\nPERFORMANCE METRICS (Last 7 days):")
            if 'tasks' in performance:
                for task in performance['tasks']:
                    report.append(f"  {task['task_id']}:")
                    report.append(f"    Executions: {task['execution_count']}")
                    report.append(f"    Avg Time: {task['avg_execution_time']}s")
                    report.append(f"    Success Rate: {task['success_rate']}%")
            
            # Data Quality
            report.append("\nDATA QUALITY METRICS:")
            if 'metrics' in quality:
                metrics = quality['metrics']
                if 'completeness' in metrics:
                    comp = metrics['completeness']
                    report.append(f"  Total Records: {comp['total_records']:,}")
                    report.append(f"  Date Coverage: {comp['date_coverage']}%")
                    report.append(f"  Fare Coverage: {comp['fare_coverage']}%")
                
                if 'fare_statistics' in metrics:
                    fares = metrics['fare_statistics']
                    report.append(f"  Average Fare: ₹{fares['average']:,.2f}")
                    report.append(f"  Fare Range: ₹{fares['minimum']:,.2f} - ₹{fares['maximum']:,.2f}")
            
            # Anomalies
            report.append("\nANOMALY DETECTION:")
            if anomalies.get('anomalies_detected', 0) > 0:
                for anomaly in anomalies['anomalies']:
                    report.append(f"  [{anomaly['severity']}] {anomaly['message']}")
            else:
                report.append("  No anomalies detected")
            
            report.append("\n" + "=" * 80)
            
            return "\n".join(report)
            
        except Exception as e:
            logger.error(f"Error generating health report: {str(e)}")
            return f"Error generating report: {str(e)}"
    
    def send_alert(self, alert_type: str, message: str, severity: str = 'INFO'):
        """
        Send alert (can be extended to email, Slack, etc.)
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity (INFO, WARNING, ERROR)
        """
        try:
            alert_data = {
                'timestamp': datetime.now().isoformat(),
                'type': alert_type,
                'severity': severity,
                'message': message
            }
            
            # Log alert
            if severity == 'ERROR':
                logger.error(f"ALERT [{alert_type}]: {message}")
            elif severity == 'WARNING':
                logger.warning(f"ALERT [{alert_type}]: {message}")
            else:
                logger.info(f"ALERT [{alert_type}]: {message}")
            
            # TODO: Integrate with external alerting systems
            # - Email via SMTP
            # - Slack webhook
            # - PagerDuty
            # - Custom notification service
            
            return alert_data
            
        except Exception as e:
            logger.error(f"Error sending alert: {str(e)}")


def main():
    """Main monitoring function"""
    monitor = PipelineMonitor()
    
    # Generate and print health report
    report = monitor.generate_health_report()
    print(report)
    
    # Get health status
    health = monitor.get_pipeline_health_status()
    
    # Send alerts if needed
    if health['overall_status'] == 'UNHEALTHY':
        monitor.send_alert(
            alert_type='PIPELINE_HEALTH',
            message='Pipeline health check failed',
            severity='ERROR'
        )
    elif health['overall_status'] == 'WARNING':
        monitor.send_alert(
            alert_type='PIPELINE_HEALTH',
            message='Pipeline health check shows warnings',
            severity='WARNING'
        )


if __name__ == '__main__':
    main()
