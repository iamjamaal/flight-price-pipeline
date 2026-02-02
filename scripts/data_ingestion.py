"""
Data Ingestion Module
Handles loading CSV data into MySQL staging database
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Tuple
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config, pipeline_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



class DataIngestionError(Exception):
    """Custom exception for data ingestion errors"""
    pass


class DataIngestion:
    """Handles data ingestion from CSV to MySQL"""
    
    def __init__(self):
        self.mysql_engine = create_engine(
            db_config.mysql_connection_string,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        logger.info("MySQL connection established")
        
        
    
    def validate_csv_file(self, file_path: str) -> bool:
        """
        Validate that CSV file exists and is readable
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            bool: True if valid, raises exception otherwise
        """
        try:
            import os
            if not os.path.exists(file_path):
                raise DataIngestionError(f"CSV file not found: {file_path}")
            
            if not os.access(file_path, os.R_OK):
                raise DataIngestionError(f"CSV file not readable: {file_path}")
            
            logger.info(f"CSV file validated: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"CSV validation failed: {str(e)}")
            raise DataIngestionError(f"CSV validation failed: {str(e)}")
    
    
    
    
    def read_csv_data(self, file_path: str) -> pd.DataFrame:
        """
        Read CSV file into pandas DataFrame
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            pd.DataFrame: Loaded data
        """
        try:
            logger.info(f"Reading CSV file: {file_path}")
            
            # First, check what columns exist
            sample_df = pd.read_csv(file_path, nrows=1)
            date_columns = []
            
            # Check for various date column formats
            if 'Date_of_Journey' in sample_df.columns:
                date_columns.append('Date_of_Journey')
            if 'Departure Date & Time' in sample_df.columns:
                date_columns.append('Departure Date & Time')
            if 'Arrival Date & Time' in sample_df.columns:
                date_columns.append('Arrival Date & Time')
            
            # Read CSV with date parsing
            df = pd.read_csv(
                file_path,
                encoding='utf-8',
                parse_dates=date_columns,
                low_memory=False
            )
            
            logger.info(f"CSV loaded successfully. Shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV: {str(e)}")
            raise DataIngestionError(f"Error reading CSV: {str(e)}")
    
    
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to match database schema
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized columns
        """
        # Create mapping of possible column names to standard names
        column_mapping = {
            # Standard format
            'Airline': 'airline',
            'Source': 'source',
            'Destination': 'destination',
            'Date_of_Journey': 'date_of_journey',
            'Dep_Time': 'departure_time',
            'Departure_Time': 'departure_time',
            'Departure Date & Time': 'departure_time',
            'Arrival_Time': 'arrival_time',
            'Arrival Date & Time': 'arrival_time',
            'Duration': 'duration',
            'Duration (hrs)': 'duration',
            'Total_Stops': 'stops',
            'Stops': 'stops',
            'Stopovers': 'stops',
            'Base Fare': 'base_fare',
            'Base_Fare': 'base_fare',
            'Base Fare (BDT)': 'base_fare',
            'Tax & Surcharge': 'tax_surcharge',
            'Tax_Surcharge': 'tax_surcharge',
            'Tax & Surcharge (BDT)': 'tax_surcharge',
            'Total Fare': 'total_fare',
            'Total_Fare': 'total_fare',
            'Total Fare (BDT)': 'total_fare',
            'Additional_Info': 'additional_info',
            'Aircraft Type': 'aircraft_type',
            'Class': 'class',
            'Booking Source': 'booking_source',
            'Seasonality': 'seasonality',
            'Days Before Departure': 'days_before_departure',
            'Source Name': 'source_name',
            'Destination Name': 'destination_name'
        }
        
        # Rename columns
        df_renamed = df.rename(columns=column_mapping)
        
        logger.info(f"Columns after standardization: {df_renamed.columns.tolist()}")
        
        return df_renamed
    
    
    
    def clean_and_prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Basic data cleaning and preparation
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            # Remove leading/trailing whitespaces
            string_columns = df.select_dtypes(include=['object']).columns
            df[string_columns] = df[string_columns].apply(lambda x: x.str.strip())
            
            # Extract date from departure_time if it contains datetime
            # This handles cases where 'Departure Date & Time' was mapped to 'departure_time'
            if 'departure_time' in df.columns and 'date_of_journey' not in df.columns:
                # If departure_time is a datetime object, extract the date
                if pd.api.types.is_datetime64_any_dtype(df['departure_time']):
                    df['date_of_journey'] = df['departure_time'].dt.date
                    logger.info("Extracted date_of_journey from departure_time")
            
            # Handle date columns
            if 'date_of_journey' in df.columns:
                df['date_of_journey'] = pd.to_datetime(df['date_of_journey'], errors='coerce')
                logger.info(f"Parsed date_of_journey. Sample: {df['date_of_journey'].head(3).tolist()}")
            
            # Handle departure and arrival times
            for time_col in ['departure_time', 'arrival_time']:
                if time_col in df.columns:
                    # If it's a datetime, extract just the time portion
                    if pd.api.types.is_datetime64_any_dtype(df[time_col]):
                        df[time_col] = df[time_col].dt.time
            
            # Convert fare columns to numeric
            fare_columns = ['base_fare', 'tax_surcharge', 'total_fare']
            for col in fare_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info("Data cleaning completed")
            
            return df
            
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise DataIngestionError(f"Error during data cleaning: {str(e)}")
    
    
    
    def truncate_staging_table(self):
        """Truncate staging table before fresh load"""
        try:
            with self.mysql_engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE staging_flights"))
                logger.info("Staging table truncated successfully")
        except Exception as e:
            logger.error(f"Error truncating table: {str(e)}")
            raise DataIngestionError(f"Error truncating table: {str(e)}")
    
    
    
    def load_to_staging(self, df: pd.DataFrame, table_name: str = 'staging_flights') -> Tuple[int, int]:
        """
        Load DataFrame to MySQL staging table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            
        Returns:
            Tuple[int, int]: (rows_inserted, rows_failed)
        """
        try:
            initial_count = len(df)
            logger.info(f"Starting data load. Total records: {initial_count}")
            
            # Select only columns that exist in the staging table
            staging_columns = [
                'airline', 'source', 'destination',
                'base_fare', 'tax_surcharge', 'total_fare',
                'date_of_journey', 'departure_time', 'arrival_time',
                'duration', 'stops'
            ]
            
            # Only include columns that exist in the dataframe
            available_columns = [col for col in staging_columns if col in df.columns]
            df_to_load = df[available_columns].copy()
            
            logger.info(f"Loading columns: {available_columns}")
            logger.info(f"Sample data shape: {df_to_load.shape}")
            
            # Load data in batches
            batch_size = pipeline_config.BATCH_SIZE
            rows_inserted = 0
            rows_failed = 0
            
            for i in range(0, len(df_to_load), batch_size):
                batch_df = df_to_load.iloc[i:i+batch_size]
                
                try:
                    batch_df.to_sql(
                        name=table_name,
                        con=self.mysql_engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    rows_inserted += len(batch_df)
                    logger.info(f"Batch {i//batch_size + 1}: Inserted {len(batch_df)} records")
                    
                except Exception as batch_error:
                    rows_failed += len(batch_df)
                    logger.error(f"Batch {i//batch_size + 1} failed: {str(batch_error)}")
            
            logger.info(f"Data load completed. Inserted: {rows_inserted}, Failed: {rows_failed}")
            
            return rows_inserted, rows_failed
            
        except Exception as e:
            logger.error(f"Error loading data to staging: {str(e)}")
            raise DataIngestionError(f"Error loading data to staging: {str(e)}")
    
    
    
    def log_ingestion_audit(self, records_processed: int, records_failed: int):
        """Log ingestion audit information"""
        try:
            with self.mysql_engine.begin() as conn:
                audit_query = text("""
                    INSERT INTO audit_log (table_name, operation, records_affected, executed_by)
                    VALUES ('staging_flights', 'INGESTION', :records, 'airflow_pipeline')
                """)
                conn.execute(audit_query, {'records': records_processed})
                
                logger.info("Audit log updated successfully")
        except Exception as e:
            logger.warning(f"Failed to log audit: {str(e)}")
    
    
    
    def execute_ingestion(self) -> dict:
        """
        Main execution method for data ingestion
        
        Returns:
            dict: Ingestion results
        """
        try:
            # Step 1: Validate CSV
            self.validate_csv_file(pipeline_config.RAW_DATA_PATH)
            
            # Step 2: Read CSV
            df = self.read_csv_data(pipeline_config.RAW_DATA_PATH)
            
            # Step 3: Standardize columns
            df = self.standardize_column_names(df)
            
            # Step 4: Clean data
            df = self.clean_and_prepare_data(df)
            
            # Step 5: Truncate staging table
            self.truncate_staging_table()
            
            # Step 6: Load to staging
            rows_inserted, rows_failed = self.load_to_staging(df)
            
            # Step 7: Log audit
            self.log_ingestion_audit(rows_inserted, rows_failed)
            
            return {
                'status': 'SUCCESS',
                'total_records': len(df),
                'rows_inserted': rows_inserted,
                'rows_failed': rows_failed
            }
            
        except Exception as e:
            logger.error(f"Data ingestion failed: {str(e)}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
        finally:
            self.mysql_engine.dispose()
            logger.info("Database connections closed")



def main():
    """Main entry point"""
    ingestion = DataIngestion()
    result = ingestion.execute_ingestion()
    
    if result['status'] == 'SUCCESS':
        logger.info(f"✓ Ingestion completed: {result}")
        return 0
    else:
        logger.error(f"✗ Ingestion failed: {result}")
        return 1


if __name__ == '__main__':
    exit(main())