"""
Data Ingestion Module
Handles loading CSV data into MySQL staging database
Supports both full refresh and incremental loading
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Tuple, Dict
from datetime import datetime
import hashlib
import os
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
    
    
    
    def generate_record_hash(self, row: pd.Series) -> str:
        """
        Generate unique hash for a record to detect changes
        
        Args:
            row: DataFrame row
            
        Returns:
            str: MD5 or SHA256 hash of key fields
        """
        try:
            # Include key fields that define record uniqueness
            key_fields = (
                f"{row.get('airline', '')}|"
                f"{row.get('source', '')}|"
                f"{row.get('destination', '')}|"
                f"{row.get('date_of_journey', '')}|"
                f"{row.get('departure_time', '')}|"
                f"{row.get('base_fare', '')}|"
                f"{row.get('total_fare', '')}"
            )
            
            if pipeline_config.HASH_ALGORITHM == 'sha256':
                return hashlib.sha256(key_fields.encode()).hexdigest()
            else:
                return hashlib.md5(key_fields.encode()).hexdigest()
                
        except Exception as e:
            logger.warning(f"Error generating hash: {str(e)}")
            return ''
    
    
    
    def get_existing_hashes(self) -> set:
        """
        Get all existing record hashes from staging table
        
        Returns:
            set: Set of existing hashes
        """
        try:
            query = "SELECT record_hash FROM staging_flights WHERE is_active = TRUE AND record_hash IS NOT NULL"
            existing_df = pd.read_sql(query, self.mysql_engine)
            existing_hashes = set(existing_df['record_hash'].tolist())
            logger.info(f"Retrieved {len(existing_hashes)} existing record hashes")
            return existing_hashes
        except Exception as e:
            logger.warning(f"Error getting existing hashes: {str(e)}")
            return set()
    
    
    
    def load_to_staging_incremental(self, df: pd.DataFrame, table_name: str = 'staging_flights') -> Tuple[int, int, int]:
        """
        Incremental load: Insert new records, mark removed records as inactive
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            
        Returns:
            Tuple[int, int, int]: (rows_inserted, rows_updated, rows_unchanged)
        """
        try:
            initial_count = len(df)
            logger.info(f"Starting incremental load. Total records in CSV: {initial_count}")
            
            # Add metadata columns
            import os
            df['source_file'] = os.path.basename(pipeline_config.RAW_DATA_PATH)
            df['ingestion_timestamp'] = datetime.now()
            df['is_active'] = True
            
            # Generate hash for each record
            logger.info("Generating record hashes...")
            df['record_hash'] = df.apply(self.generate_record_hash, axis=1)
            
            # Get existing hashes from database
            existing_hashes = self.get_existing_hashes()
            
            # Classify records
            new_records_mask = ~df['record_hash'].isin(existing_hashes)
            new_records = df[new_records_mask]
            existing_records = df[~new_records_mask]
            
            rows_inserted = 0
            rows_unchanged = len(existing_records)
            
            # Insert new records in batches
            if len(new_records) > 0:
                logger.info(f"Inserting {len(new_records)} new records...")
                
                # Select columns for insertion
                staging_columns = [
                    'airline', 'source', 'destination',
                    'base_fare', 'tax_surcharge', 'total_fare',
                    'date_of_journey', 'departure_time', 'arrival_time',
                    'duration', 'stops',
                    'record_hash', 'source_file', 'ingestion_timestamp', 'is_active'
                ]
                
                available_columns = [col for col in staging_columns if col in new_records.columns]
                new_records_to_load = new_records[available_columns].copy()
                
                # Batch insert
                batch_size = pipeline_config.BATCH_SIZE
                for i in range(0, len(new_records_to_load), batch_size):
                    batch_df = new_records_to_load.iloc[i:i+batch_size]
                    
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
                        logger.error(f"Batch {i//batch_size + 1} failed: {str(batch_error)}")
            
            # Mark records not in new batch as inactive (soft delete)
            incoming_hashes = set(df['record_hash'].tolist())
            hashes_to_deactivate = existing_hashes - incoming_hashes
            
            if hashes_to_deactivate:
                logger.info(f"Marking {len(hashes_to_deactivate)} removed records as inactive...")
                
                # Batch deactivate to avoid SQL parameter limits
                deactivate_batch_size = 1000
                hashes_list = list(hashes_to_deactivate)
                
                for i in range(0, len(hashes_list), deactivate_batch_size):
                    batch_hashes = hashes_list[i:i+deactivate_batch_size]
                    placeholders = ','.join([':hash' + str(j) for j in range(len(batch_hashes))])
                    
                    with self.mysql_engine.begin() as conn:
                        deactivate_query = f"""
                            UPDATE staging_flights 
                            SET is_active = FALSE 
                            WHERE record_hash IN ({placeholders})
                        """
                        params = {f'hash{j}': h for j, h in enumerate(batch_hashes)}
                        conn.execute(text(deactivate_query), params)
                
                logger.info(f"Deactivated {len(hashes_to_deactivate)} records")
            
            logger.info(f"Incremental load completed: {rows_inserted} new, {rows_unchanged} unchanged, "
                       f"{len(hashes_to_deactivate)} deactivated")
            
            return rows_inserted, 0, rows_unchanged
            
        except Exception as e:
            logger.error(f"Incremental load failed: {str(e)}")
            raise DataIngestionError(f"Incremental load failed: {str(e)}")
    
    
    
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
    
    
    
    
    def should_use_incremental_load(self) -> bool:
        """
        Determine if incremental load should be used
        
        Returns:
            bool: True if incremental, False for full refresh
        """
        # Check configuration
        if not pipeline_config.USE_INCREMENTAL_LOAD:
            logger.info("Incremental loading disabled in configuration")
            return False
        
        # Check if it's full refresh day (e.g., Sunday)
        from datetime import datetime
        today_weekday = datetime.now().weekday()
        
        # Convert Sunday (6 in Python) to 0 for comparison
        if today_weekday == 6:
            today_weekday = 0
        
        if today_weekday == pipeline_config.FULL_REFRESH_DAY:
            logger.info(f"Today is full refresh day (weekday {pipeline_config.FULL_REFRESH_DAY})")
            return False
        
        logger.info("Using incremental load")
        return True
    
    
    
    def execute_ingestion(self) -> Dict:
        """
        Main execution method for data ingestion
        Supports both full refresh and incremental loading
        
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
            
            # Step 5: Determine load strategy
            use_incremental = self.should_use_incremental_load()
            
            if use_incremental:
                # Incremental load
                logger.info("Executing INCREMENTAL load...")
                rows_inserted, rows_updated, rows_unchanged = self.load_to_staging_incremental(df)
                rows_failed = 0
                load_mode = 'INCREMENTAL'
            else:
                # Full refresh
                logger.info("Executing FULL REFRESH...")
                self.truncate_staging_table()
                rows_inserted, rows_failed = self.load_to_staging(df)
                rows_updated = 0
                rows_unchanged = 0
                load_mode = 'FULL_REFRESH'
            
            # Step 6: Log audit
            self.log_ingestion_audit(rows_inserted, rows_failed)
            
            return {
                'status': 'SUCCESS',
                'load_mode': load_mode,
                'total_records': len(df),
                'rows_inserted': rows_inserted,
                'rows_updated': rows_updated,
                'rows_unchanged': rows_unchanged,
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