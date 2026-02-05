"""
Monitoring Dashboard DAG
Runs monitoring checks and generates health reports
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')

from monitoring import PipelineMonitor

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monitoring_dashboard',
    default_args=default_args,
    description='Pipeline monitoring and health checks',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['monitoring', 'observability'],
)


def check_pipeline_health(**context):
    """Check overall pipeline health"""
    monitor = PipelineMonitor()
    health_status = monitor.get_pipeline_health_status()
    
    print("\n" + "="*80)
    print("PIPELINE HEALTH STATUS")
    print("="*80)
    print(f"Overall Status: {health_status['overall_status']}")
    print(f"Timestamp: {health_status['timestamp']}")
    
    for component, status in health_status.get('components', {}).items():
        print(f"\n{component.upper()}:")
        print(f"  Status: {status['status']}")
        if 'message' in status:
            print(f"  {status['message']}")
    
    # Push status to XCom for downstream tasks
    context['ti'].xcom_push(key='health_status', value=health_status['overall_status'])
    
    return health_status['overall_status']


def collect_performance_metrics(**context):
    """Collect performance metrics"""
    monitor = PipelineMonitor()
    metrics = monitor.get_performance_metrics()
    
    print("\n" + "="*80)
    print("PERFORMANCE METRICS")
    print("="*80)
    print(f"Period: {metrics.get('period', 'N/A')}")
    
    for task in metrics.get('tasks', []):
        print(f"\nTask: {task['task_id']}")
        print(f"  Executions: {task['execution_count']}")
        print(f"  Avg Time: {task['avg_execution_time']}s")
        print(f"  Success Rate: {task['success_rate']}%")
    
    return metrics


def assess_data_quality(**context):
    """Assess data quality"""
    monitor = PipelineMonitor()
    quality = monitor.get_data_quality_metrics()
    
    print("\n" + "="*80)
    print("DATA QUALITY METRICS")
    print("="*80)
    
    metrics = quality.get('metrics', {})
    
    if 'completeness' in metrics:
        comp = metrics['completeness']
        print(f"\nCompleteness:")
        print(f"  Total Records: {comp['total_records']:,}")
        print(f"  Date Coverage: {comp['date_coverage']}%")
        print(f"  Fare Coverage: {comp['fare_coverage']}%")
    
    if 'validation_history' in metrics:
        print(f"\nValidation History:")
        for check in metrics['validation_history']:
            print(f"  {check['check_name']}: {check['pass_rate']}% pass rate")
    
    return quality


def detect_data_anomalies(**context):
    """Detect anomalies in data"""
    monitor = PipelineMonitor()
    anomalies = monitor.detect_anomalies()
    
    print("\n" + "="*80)
    print("ANOMALY DETECTION")
    print("="*80)
    print(f"Anomalies Detected: {anomalies.get('anomalies_detected', 0)}")
    
    for anomaly in anomalies.get('anomalies', []):
        print(f"\n[{anomaly['severity']}] {anomaly['type']}")
        print(f"  {anomaly['message']}")
    
    # Send alerts for critical anomalies
    critical_anomalies = [
        a for a in anomalies.get('anomalies', []) 
        if a['severity'] in ['ERROR', 'WARNING']
    ]
    
    if critical_anomalies:
        for anomaly in critical_anomalies:
            monitor.send_alert(
                alert_type=anomaly['type'],
                message=anomaly['message'],
                severity=anomaly['severity']
            )
    
    return anomalies


def generate_health_report(**context):
    """Generate comprehensive health report"""
    monitor = PipelineMonitor()
    report = monitor.generate_health_report()
    
    print(report)
    
    # Save report to file
    report_dir = '/opt/airflow/logs/monitoring'
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'{report_dir}/health_report_{timestamp}.txt'
    
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"\nReport saved to: {report_file}")
    
    return report_file


def send_alerts_if_needed(**context):
    """Send alerts based on health status"""
    monitor = PipelineMonitor()
    health_status = context['ti'].xcom_pull(key='health_status', task_ids='check_health')
    
    if health_status == 'UNHEALTHY':
        monitor.send_alert(
            alert_type='PIPELINE_HEALTH',
            message='Pipeline health check FAILED - immediate attention required',
            severity='ERROR'
        )
        print("CRITICAL ALERT SENT: Pipeline is UNHEALTHY")
    elif health_status == 'WARNING':
        monitor.send_alert(
            alert_type='PIPELINE_HEALTH',
            message='Pipeline health check shows WARNING status',
            severity='WARNING'
        )
        print("WARNING ALERT SENT: Pipeline has warnings")
    else:
        print("No alerts needed - Pipeline is HEALTHY")


# Define tasks
task_check_health = PythonOperator(
    task_id='check_health',
    python_callable=check_pipeline_health,
    dag=dag,
)

task_performance_metrics = PythonOperator(
    task_id='collect_performance_metrics',
    python_callable=collect_performance_metrics,
    dag=dag,
)

task_data_quality = PythonOperator(
    task_id='assess_data_quality',
    python_callable=assess_data_quality,
    dag=dag,
)

task_anomaly_detection = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_data_anomalies,
    dag=dag,
)

task_generate_report = PythonOperator(
    task_id='generate_health_report',
    python_callable=generate_health_report,
    dag=dag,
)

task_send_alerts = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts_if_needed,
    dag=dag,
)

# Define task dependencies
task_check_health >> [task_performance_metrics, task_data_quality, task_anomaly_detection]
[task_performance_metrics, task_data_quality, task_anomaly_detection] >> task_generate_report
task_generate_report >> task_send_alerts
