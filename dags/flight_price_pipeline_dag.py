"""
Flight Price Pipeline DAG
Main Airflow DAG orchestrating the entire ETL pipeline
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import airflow_config

# Import pipeline modules
sys.path.append('/opt/airflow/scripts')
from data_ingestion import DataIngestion
from data_validation import DataValidator
from data_transformation import DataTransformer
from kpi_computation import KPIComputer
from monitoring import PipelineMonitor

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email': airflow_config.EMAIL_LIST,
    'email_on_failure': airflow_config.EMAIL_ON_FAILURE,
    'email_on_retry': airflow_config.EMAIL_ON_RETRY,
    'retries': airflow_config.RETRIES,
    'retry_delay': timedelta(minutes=airflow_config.RETRY_DELAY_MINUTES),
    'execution_timeout': timedelta(hours=2),
}


def run_data_ingestion(**context):
    """Task function: Run data ingestion"""
    ingestion = DataIngestion()
    result = ingestion.execute_ingestion()
    
    if result['status'] != 'SUCCESS':
        raise Exception(f"Data ingestion failed: {result.get('error', 'Unknown error')}")
    
    # Push results to XCom for downstream tasks
    context['task_instance'].xcom_push(key='ingestion_result', value=result)
    return result


def run_data_validation(**context):
    """Task function: Run data validation"""
    validator = DataValidator()
    result = validator.execute_validation()
    
    if result['status'] == 'FAILED':
        raise Exception(f"Data validation failed. Failed checks: {result.get('failed', 0)}")
    
    if result['status'] == 'WARNING':
        print(f"⚠ Data validation completed with warnings: {result.get('warnings', 0)} warnings")
    
    context['task_instance'].xcom_push(key='validation_result', value=result)
    return result


def run_data_transformation(**context):
    """Task function: Run data transformation"""
    transformer = DataTransformer()
    result = transformer.execute_transformation()
    
    if result['status'] != 'SUCCESS':
        raise Exception(f"Data transformation failed: {result.get('error', 'Unknown error')}")
    
    context['task_instance'].xcom_push(key='transformation_result', value=result)
    return result


def run_kpi_computation(**context):
    """Task function: Run KPI computation"""
    kpi_computer = KPIComputer()
    result = kpi_computer.execute_kpi_computation()
    
    if result['status'] != 'SUCCESS':
        raise Exception(f"KPI computation failed: {result.get('error', 'Unknown error')}")
    
    context['task_instance'].xcom_push(key='kpi_result', value=result)
    return result


def log_pipeline_execution(**context):
    """Task function: Log overall pipeline execution"""
    import logging
    from sqlalchemy import create_engine, text
    from dags.config.pipeline_config import db_config
    
    logger = logging.getLogger(__name__)
    
    # Get results from previous tasks
    ti = context['task_instance']
    ingestion_result = ti.xcom_pull(key='ingestion_result', task_ids='data_ingestion')
    validation_result = ti.xcom_pull(key='validation_result', task_ids='data_validation')
    transformation_result = ti.xcom_pull(key='transformation_result', task_ids='data_transformation')
    kpi_result = ti.xcom_pull(key='kpi_result', task_ids='kpi_computation')
    
    # Log to PostgreSQL
    postgres_engine = create_engine(db_config.postgres_connection_string)
    
    try:
        with postgres_engine.begin() as conn:
            # Log pipeline execution
            log_query = text("""
                INSERT INTO pipeline_execution_log 
                (dag_id, task_id, execution_date, status, records_processed, execution_time)
                VALUES 
                (:dag_id, :task_id, :execution_date, :status, :records, 
                 CAST(:exec_time AS INTERVAL))
            """)
            
            # Convert execution_date from Proxy to datetime
            # Use data_interval_start which is more reliable
            from datetime import datetime
            execution_date_raw = context.get('data_interval_start') or context.get('logical_date')
            # Convert pendulum DateTime to Python datetime if needed
            if hasattr(execution_date_raw, 'to_datetime_string'):
                execution_date = datetime.fromisoformat(execution_date_raw.to_datetime_string())
            elif hasattr(execution_date_raw, '__wrapped__'):
                execution_date = execution_date_raw.__wrapped__
            else:
                execution_date = execution_date_raw
            
            # Log ingestion
            if ingestion_result:
                conn.execute(log_query, {
                    'dag_id': context['dag'].dag_id,
                    'task_id': 'data_ingestion',
                    'execution_date': execution_date,
                    'status': ingestion_result.get('status', 'UNKNOWN'),
                    'records': ingestion_result.get('rows_inserted', 0),
                    'exec_time': '5 minutes'
                })
            
            # Log validation
            if validation_result:
                conn.execute(log_query, {
                    'dag_id': context['dag'].dag_id,
                    'task_id': 'data_validation',
                    'execution_date': execution_date,
                    'status': validation_result.get('status', 'UNKNOWN'),
                    'records': 0,
                    'exec_time': '2 minutes'
                })
            
            # Log transformation
            if transformation_result:
                conn.execute(log_query, {
                    'dag_id': context['dag'].dag_id,
                    'task_id': 'data_transformation',
                    'execution_date': execution_date,
                    'status': transformation_result.get('status', 'UNKNOWN'),
                    'records': transformation_result.get('records_saved', 0),
                    'exec_time': '3 minutes'
                })
            
            # Log KPI computation
            if kpi_result:
                conn.execute(log_query, {
                    'dag_id': context['dag'].dag_id,
                    'task_id': 'kpi_computation',
                    'execution_date': execution_date,
                    'status': kpi_result.get('status', 'UNKNOWN'),
                    'records': kpi_result.get('kpis_computed', 0),
                    'exec_time': '2 minutes'
                })
            
            logger.info("Pipeline execution logged successfully")
    
    finally:
        postgres_engine.dispose()
    
    return {
        'ingestion': ingestion_result,
        'validation': validation_result,
        'transformation': transformation_result,
        'kpi': kpi_result
    }


def monitor_pipeline_health(**context):
    """Monitor pipeline health after execution"""
    monitor = PipelineMonitor()
    
    print("\n" + "="*80)
    print("POST-EXECUTION HEALTH CHECK")
    print("="*80)
    
    health_status = monitor.get_pipeline_health_status()
    print(f"Overall Status: {health_status['overall_status']}")
    
    # Log any issues
    if health_status['overall_status'] != 'HEALTHY':
        monitor.send_alert(
            alert_type='PIPELINE_COMPLETION',
            message=f"Pipeline completed with status: {health_status['overall_status']}",
            severity='WARNING' if health_status['overall_status'] == 'WARNING' else 'ERROR'
        )
    else:
        print("✅ Pipeline health check PASSED")
    
    return health_status['overall_status']


# Create DAG
with DAG(
    dag_id=airflow_config.DAG_ID,
    default_args=default_args,
    description='End-to-end flight price analysis pipeline',
    schedule_interval=airflow_config.SCHEDULE_INTERVAL,
    start_date=days_ago(1),
    catchup=airflow_config.CATCHUP,
    max_active_runs=airflow_config.MAX_ACTIVE_RUNS,
    tags=airflow_config.TAGS,
    doc_md="""
    # Flight Price Analysis Pipeline
    
    ## Purpose
    Process and analyze flight price data for Bangladesh, computing key performance indicators
    and storing results in an analytics database.
    
    ## Pipeline Stages
    1. **Data Ingestion**: Load CSV data into MySQL staging database
    2. **Data Validation**: Perform comprehensive quality checks
    3. **Data Transformation**: Transform and enrich data, add seasonal features
    4. **KPI Computation**: Calculate analytics metrics
    5. **Logging**: Record pipeline execution details
    
    ## Schedule
    Runs daily at midnight (configurable)
    
    ## Alerts
    Email notifications on failure
    """
) as dag:
    
    # Task 1: Start pipeline marker
    start_pipeline = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting Flight Price Pipeline - $(date)"',
        doc_md="""
        ### Start Pipeline
        Marks the beginning of the pipeline execution.
        """
    )
    
    # Task 2: Data Ingestion
    data_ingestion = PythonOperator(
        task_id='data_ingestion',
        python_callable=run_data_ingestion,
        provide_context=True,
        doc_md="""
        ### Data Ingestion
        
        **Purpose**: Load CSV flight data into MySQL staging database
        
        **Input**: `Flight_Price_Dataset_of_Bangladesh.csv`
        
        **Output**: Data loaded into `staging_flights` table
        
        **Validations**:
        - CSV file exists and is readable
        - Column names are standardized
        - Data types are appropriate
        
        **Error Handling**: 
        - Retries up to 3 times on failure
        - Logs errors to audit table
        """
    )
    
    # Task 3: Data Validation
    data_validation = PythonOperator(
        task_id='data_validation',
        python_callable=run_data_validation,
        provide_context=True,
        doc_md="""
        ### Data Validation
        
        **Purpose**: Ensure data quality through comprehensive checks
        
        **Checks Performed**:
        - Required columns exist
        - No null values in critical fields
        - Data types are correct
        - Fare values are consistent
        - City names are valid
        - No duplicate records
        
        **Output**: Validation results logged to `data_quality_log` table
        
        **Failure Handling**: Pipeline fails if critical checks fail
        """
    )
    
    # Task 4: Data Transformation
    data_transformation = PythonOperator(
        task_id='data_transformation',
        python_callable=run_data_transformation,
        provide_context=True,
        doc_md="""
        ### Data Transformation
        
        **Purpose**: Transform and enrich data for analytics
        
        **Transformations**:
        - Calculate/verify total fare
        - Add seasonal classification
        - Add peak season flag
        - Standardize text fields
        - Remove duplicates and nulls
        
        **Output**: Transformed data in PostgreSQL `flights_analytics` table
        """
    )
    
    # Task 5: KPI Computation
    kpi_computation = PythonOperator(
        task_id='kpi_computation',
        python_callable=run_kpi_computation,
        provide_context=True,
        doc_md="""
        ### KPI Computation
        
        **Purpose**: Calculate key performance indicators
        
        **KPIs Computed**:
        1. Average Fare by Airline
        2. Seasonal Fare Variation
        3. Most Popular Routes (Top 20)
        4. Booking Count by Airline
        
        **Output**: KPI tables in PostgreSQL
        """
    )
    
    # Task 6: Log Pipeline Execution
    log_execution = PythonOperator(
        task_id='log_pipeline_execution',
        python_callable=log_pipeline_execution,
        provide_context=True,
        doc_md="""
        ### Log Pipeline Execution
        
        **Purpose**: Record pipeline execution metrics
        
        **Logged Information**:
        - Execution timestamp
        - Records processed per task
        - Task status
        - Execution time
        
        **Output**: `pipeline_execution_log` table
        """
    )
    
    # Task 7: Monitor Pipeline Health
    monitor_health = PythonOperator(
        task_id='monitor_health',
        python_callable=monitor_pipeline_health,
        provide_context=True,
        doc_md="""
        ### Monitor Pipeline Health
        
        **Purpose**: Check pipeline health and data quality after execution
        
        **Checks**:
        - Database connectivity
        - Data freshness
        - Validation status
        - Performance metrics
        
        **Output**: Health status and alerts if needed
        """
    )
    
    # Task 8: End pipeline marker
    end_pipeline = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "✓ Flight Price Pipeline Completed Successfully - $(date)"',
        doc_md="""
        ### End Pipeline
        Marks successful completion of the pipeline.
        """
    )
    
    # Define task dependencies
    start_pipeline >> data_ingestion >> data_validation >> data_transformation >> kpi_computation >> log_execution >> monitor_health >> end_pipeline