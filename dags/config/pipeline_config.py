"""
Configuration module for Flight Price Pipeline
Centralizes all configuration settings
"""
import os
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class DatabaseConfig:
    """Database connection configurations"""
    
    # MySQL Staging Database
    MYSQL_HOST: str = os.getenv('MYSQL_HOST', 'mysql-staging')
    MYSQL_PORT: int = int(os.getenv('MYSQL_PORT', 3306))
    MYSQL_DATABASE: str = os.getenv('MYSQL_DATABASE', 'staging_db')
    MYSQL_USER: str = os.getenv('MYSQL_USER', 'staging_user')
    MYSQL_PASSWORD: str = os.getenv('MYSQL_PASSWORD', 'staging_pass')
    
    # PostgreSQL Analytics Database
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'postgres-analytics')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DATABASE: str = os.getenv('POSTGRES_DATABASE', 'analytics_db')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'analytics_user')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'analytics_pass')
    
    
    @property
    def mysql_connection_string(self) -> str:
        return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"
    
    @property
    def postgres_connection_string(self) -> str:
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DATABASE}"


@dataclass
class PipelineConfig:
    """Pipeline-specific configurations"""
    
    # Data paths
    DATA_DIR: str = '/opt/airflow/data'
    RAW_DATA_PATH: str = os.path.join(DATA_DIR, 'raw', 'Flight_Price_Dataset_of_Bangladesh.csv')
    PROCESSED_DATA_PATH: str = os.path.join(DATA_DIR, 'processed')
    
    # Required columns
    REQUIRED_COLUMNS: List[str] = None
    
    # Season definitions for Bangladesh
    PEAK_SEASONS: Dict[str, List[tuple]] = None
    
    # Data quality thresholds
    MAX_NULL_PERCENTAGE: float = 5.0  # Maximum 5% null values allowed
    MIN_FARE_VALUE: float = 0.0
    MAX_FARE_VALUE: float = 1000000.0  # 1 million BDT
    
    # Batch processing
    BATCH_SIZE: int = 1000
    
    # Incremental loading settings
    USE_INCREMENTAL_LOAD: bool = os.getenv('USE_INCREMENTAL_LOAD', 'true').lower() == 'true'
    FULL_REFRESH_DAY: int = int(os.getenv('FULL_REFRESH_DAY', 0))  # 0 = Sunday for weekly full refresh
    HASH_ALGORITHM: str = 'md5'  # md5, sha256
    ENABLE_HISTORY_TRACKING: bool = True  # Track record changes over time
    
    def __post_init__(self):
        """Initialize complex default values"""
        if self.REQUIRED_COLUMNS is None:
            self.REQUIRED_COLUMNS = [
                'airline', 'source', 'destination', 
                'base_fare', 'tax_surcharge', 'total_fare'
            ]
        
        if self.PEAK_SEASONS is None:
            # Define peak seasons for Bangladesh
            # Eid-ul-Fitr (April-May), Eid-ul-Adha (July), Winter holidays (Dec-Jan)
            self.PEAK_SEASONS = {
                'Eid-ul-Fitr': [(4, 1), (5, 31)],  # April-May
                'Eid-ul-Adha': [(7, 1), (7, 31)],  # July
                'Winter': [(12, 1), (1, 31)],       # December-January
                'Durga Puja': [(10, 1), (10, 31)]  # October
            }


@dataclass
class AirflowConfig:
    """Airflow-specific configurations"""
    
    DAG_ID: str = 'flight_price_pipeline'
    SCHEDULE_INTERVAL: str = '@daily'
    START_DATE: str = '2026-02-04'
    CATCHUP: bool = False
    MAX_ACTIVE_RUNS: int = 1
    TAGS: List[str] = None
    
    # Email notifications
    EMAIL_ON_FAILURE: bool = True
    EMAIL_ON_RETRY: bool = True
    EMAIL_LIST: List[str] = None
    
    # Retry configuration
    RETRIES: int = 3
    RETRY_DELAY_MINUTES: int = 5
    
    def __post_init__(self):
        if self.TAGS is None:
            self.TAGS = ['flight_price', 'analytics', 'etl']
        
        if self.EMAIL_LIST is None:
            self.EMAIL_LIST = ['data-team@example.com']


# Create singleton instances
db_config = DatabaseConfig()
pipeline_config = PipelineConfig()
airflow_config = AirflowConfig()