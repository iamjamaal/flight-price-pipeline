"""
Data Transformation Module
Handles data transformation and seasonal classification
Supports both full refresh and incremental (CDC) patterns
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Dict, Tuple
from datetime import datetime
import hashlib
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config, pipeline_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Custom exception for transformation errors"""
    pass


class DataTransformer:
    """Handles data transformation operations"""
    
    def __init__(self):
        self.mysql_engine = create_engine(db_config.mysql_connection_string, pool_pre_ping=True)
        self.postgres_engine = create_engine(db_config.postgres_connection_string, pool_pre_ping=True)
        logger.info("Data Transformer initialized")
    
    def load_staging_data(self) -> pd.DataFrame:
        """Load validated data from staging (active records only for incremental)"""
        try:
            if pipeline_config.USE_INCREMENTAL_LOAD:
                # Load only active records for incremental processing
                query = "SELECT * FROM staging_flights WHERE is_active = TRUE"
                logger.info("Loading active records from staging (incremental mode)")
            else:
                # Load all records for full refresh
                query = "SELECT * FROM staging_flights"
                logger.info("Loading all records from staging (full refresh mode)")
            
            df = pd.read_sql(query, self.mysql_engine)
            logger.info(f"Loaded {len(df)} records from staging for transformation")
            return df
        except Exception as e:
            logger.error(f"Error loading staging data: {str(e)}")
            raise TransformationError(f"Error loading staging data: {str(e)}")
    
    
    
    def load_staging_data_incremental(self) -> pd.DataFrame:
        """Load only new/updated records since last successful run"""
        try:
            # Get last successful transformation timestamp
            last_run_query = """
                SELECT MAX(execution_date) as last_run 
                FROM pipeline_execution_log 
                WHERE task_id = 'data_transformation' 
                AND status = 'SUCCESS'
            """
            
            with self.postgres_engine.connect() as conn:
                result = conn.execute(text(last_run_query)).fetchone()
                last_run = result[0] if result and result[0] else datetime(1970, 1, 1)
            
            logger.info(f"Last successful transformation: {last_run}")
            
            # Load records ingested after last run
            # Convert datetime to string for MySQL compatibility
            last_run_str = last_run.strftime('%Y-%m-%d %H:%M:%S')
            
            query = f"""
                SELECT * FROM staging_flights 
                WHERE is_active = TRUE 
                AND ingestion_timestamp > '{last_run_str}'
            """
            
            df = pd.read_sql(query, self.mysql_engine)
            logger.info(f"Loaded {len(df)} incremental records from staging")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading incremental data: {str(e)}")
            # Fallback to all active records if error
            logger.warning("Falling back to loading all active records")
            return self.load_staging_data()
    
    
    
    def generate_record_hash(self, row: pd.Series) -> str:
        """Generate unique hash for a record"""
        try:
            key_fields = (
                f"{row.get('airline', '')}|"
                f"{row.get('source', '')}|"
                f"{row.get('destination', '')}|"
                f"{row.get('date_of_journey', '')}|"
                f"{row.get('departure_time', '')}"
            )
            
            if pipeline_config.HASH_ALGORITHM == 'sha256':
                return hashlib.sha256(key_fields.encode()).hexdigest()
            else:
                return hashlib.md5(key_fields.encode()).hexdigest()
        except Exception as e:
            logger.warning(f"Error generating hash: {str(e)}")
            return ''
    
    
    def calculate_total_fare(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate total fare if not present or recalculate if inconsistent
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with calculated total_fare
        """
        try:
            # Ensure numeric types
            df['base_fare'] = pd.to_numeric(df['base_fare'], errors='coerce')
            df['tax_surcharge'] = pd.to_numeric(df['tax_surcharge'], errors='coerce')
            
            # Calculate total fare
            df['total_fare_calculated'] = df['base_fare'] + df['tax_surcharge']
            
            # If total_fare column exists, verify it matches calculated value
            if 'total_fare' in df.columns:
                df['total_fare'] = pd.to_numeric(df['total_fare'], errors='coerce')
                # Replace mismatched or null values with calculated values
                mismatch_mask = ((df['total_fare'] - df['total_fare_calculated']).abs() > 0.01) | df['total_fare'].isna()
                mismatch_count = mismatch_mask.sum()
                if mismatch_count > 0:
                    logger.warning(f"Found {mismatch_count} fare mismatches. Recalculating...")
                    df.loc[mismatch_mask, 'total_fare'] = df.loc[mismatch_mask, 'total_fare_calculated']
            else:
                # Create total_fare column
                df['total_fare'] = df['total_fare_calculated']
            
            # Drop temporary column
            df = df.drop('total_fare_calculated', axis=1)
            
            logger.info("Total fare calculation completed")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating total fare: {str(e)}")
            raise TransformationError(f"Error calculating total fare: {str(e)}")
    
    
    
    def classify_season(self, date_obj) -> str:
        """
        Classify a date into a season
        
        Args:
            date_obj: Date to classify
            
        Returns:
            str: Season name
        """
        if pd.isna(date_obj):
            return 'Unknown'
        
        month = date_obj.month
        day = date_obj.day
        
        # Winter: December-January
        if month in [12, 1]:
            return 'Winter'
        # Summer: March-May
        elif month in [3, 4, 5]:
            return 'Summer'
        # Monsoon: June-September
        elif month in [6, 7, 8, 9]:
            return 'Monsoon'
        # Autumn: October-November
        elif month in [10, 11]:
            return 'Autumn'
        # Spring: February
        else:
            return 'Spring'
    
    
    
    def determine_peak_season(self, date_obj, season: str) -> bool:
        """
        Determine if a date falls within peak travel season
        
        Args:
            date_obj: Date to check
            season: Season classification
            
        Returns:
            bool: True if peak season, False otherwise
        """
        if pd.isna(date_obj):
            return False
        
        month = date_obj.month
        
        # Peak seasons in Bangladesh:
        # 1. Eid-ul-Fitr (April-May)
        # 2. Eid-ul-Adha (July)
        # 3. Durga Puja (October)
        # 4. Winter holidays (December)
        
        peak_months = [4, 5, 7, 10, 12]
        return month in peak_months
    
    
    
    def add_seasonal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add seasonal features to the dataframe
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with seasonal features
        """
        try:
            # Ensure date_of_journey is datetime
            df['date_of_journey'] = pd.to_datetime(df['date_of_journey'], errors='coerce')
            
            # Add season classification
            df['season'] = df['date_of_journey'].apply(self.classify_season)
            
            # Add peak season flag
            df['is_peak_season'] = df.apply(
                lambda row: self.determine_peak_season(row['date_of_journey'], row['season']),
                axis=1
            )
            
            logger.info(f"Seasonal features added. Peak season records: {df['is_peak_season'].sum()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error adding seasonal features: {str(e)}")
            raise TransformationError(f"Error adding seasonal features: {str(e)}")
    
    
    
    def clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final cleaning and standardization
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            # Remove records with critical null values
            critical_columns = ['airline', 'source', 'destination', 'total_fare']
            initial_count = len(df)
            df = df.dropna(subset=critical_columns)
            removed_count = initial_count - len(df)
            
            if removed_count > 0:
                logger.warning(f"Removed {removed_count} records with null critical values")
            
            # Standardize text fields (title case)
            text_columns = ['airline', 'source', 'destination']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].str.title()
            
            # Remove duplicates if any
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicate_count = initial_count - len(df)
            
            if duplicate_count > 0:
                logger.warning(f"Removed {duplicate_count} duplicate records")
            
            logger.info(f"Final record count after cleaning: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error during final cleaning: {str(e)}")
            raise TransformationError(f"Error during final cleaning: {str(e)}")
    
    
    
    def save_to_analytics_db(self, df: pd.DataFrame, table_name: str = 'flights_analytics') -> int:
        """
        Save transformed data to PostgreSQL analytics database
        
        Args:
            df: DataFrame to save
            table_name: Target table name
            
        Returns:
            int: Number of records saved
        """
        try:
            # Select and order columns
            columns_to_save = [
                'airline', 'source', 'destination',
                'base_fare', 'tax_surcharge', 'total_fare',
                'date_of_journey', 'departure_time', 'arrival_time',
                'duration', 'stops', 'season', 'is_peak_season'
            ]
            
            # Only include columns that exist in the dataframe
            available_columns = [col for col in columns_to_save if col in df.columns]
            df_to_save = df[available_columns].copy()
            
            # Convert time columns from bigint (microseconds) to TIME format
            for time_col in ['departure_time', 'arrival_time']:
                if time_col in df_to_save.columns:
                    # Convert microseconds to timedelta then to time
                    df_to_save[time_col] = pd.to_timedelta(df_to_save[time_col], unit='us').apply(
                        lambda x: (pd.Timestamp('1970-01-01') + x).time() if pd.notna(x) else None
                    )
            
            # Clear existing data
            with self.postgres_engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                logger.info(f"Truncated {table_name} table")
            
            # Save data in batches
            batch_size = pipeline_config.BATCH_SIZE  # 5000 records per batch
            total_saved = 0
            
            for i in range(0, len(df_to_save), batch_size):
                batch_df = df_to_save.iloc[i:i+batch_size].copy()
                batch_df.to_sql(
                    name=table_name,
                    con=self.postgres_engine,
                    if_exists='append',
                    index=False
                )
                total_saved += len(batch_df)
                logger.info(f"Saved batch {i//batch_size + 1}: {len(batch_df)} records")
            
            logger.info(f"Total records saved to {table_name}: {total_saved}")
            
            return total_saved
            
        except Exception as e:
            logger.error(f"Error saving to analytics database: {str(e)}")
            raise TransformationError(f"Error saving to analytics database: {str(e)}")
    
    
    
    def save_to_analytics_db_incremental(self, df: pd.DataFrame, table_name: str = 'flights_analytics') -> Tuple[int, int]:
        """
        Save data incrementally using UPSERT (INSERT ... ON CONFLICT UPDATE)
        
        Args:
            df: DataFrame to save
            table_name: Target table name
            
        Returns:
            Tuple[int, int]: (rows_inserted, rows_updated)
        """
        try:
            if len(df) == 0:
                logger.info("No new records to process")
                return 0, 0
            
            # Prepare columns
            columns_to_save = [
                'airline', 'source', 'destination',
                'base_fare', 'tax_surcharge', 'total_fare',
                'date_of_journey', 'departure_time', 'arrival_time',
                'duration', 'stops', 'season', 'is_peak_season'
            ]
            
            # Only include columns that exist
            available_columns = [col for col in columns_to_save if col in df.columns]
            df_to_save = df[available_columns].copy()
            
            # Add tracking columns
            df_to_save['record_hash'] = df.apply(self.generate_record_hash, axis=1)
            df_to_save['last_updated_date'] = datetime.now()
            df_to_save['is_active'] = True
            
            # Convert time columns from bigint to TIME format
            for time_col in ['departure_time', 'arrival_time']:
                if time_col in df_to_save.columns:
                    df_to_save[time_col] = pd.to_timedelta(df_to_save[time_col], unit='us').apply(
                        lambda x: (pd.Timestamp('1970-01-01') + x).time() if pd.notna(x) else None
                    )
            
            rows_inserted = 0
            rows_updated = 0
            
            # Process in batches
            batch_size = pipeline_config.BATCH_SIZE
            
            for i in range(0, len(df_to_save), batch_size):
                batch_df = df_to_save.iloc[i:i+batch_size].copy()
                
                # Create temporary table for batch
                temp_table = f'temp_flights_batch_{i}'
                batch_df.to_sql(temp_table, self.postgres_engine, if_exists='replace', index=False)
                
                # Perform UPSERT using ON CONFLICT
                upsert_query = f"""
                    WITH upsert AS (
                        INSERT INTO {table_name} (
                            airline, source, destination,
                            base_fare, tax_surcharge, total_fare,
                            date_of_journey, departure_time, arrival_time,
                            duration, stops, season, is_peak_season,
                            record_hash, last_updated_date, is_active
                        )
                        SELECT 
                            airline, source, destination,
                            base_fare, tax_surcharge, total_fare,
                            date_of_journey, departure_time, arrival_time,
                            duration, stops, season, is_peak_season,
                            record_hash, last_updated_date, is_active
                        FROM {temp_table}
                        ON CONFLICT (airline, source, destination, date_of_journey, departure_time) 
                        DO UPDATE SET
                            base_fare = EXCLUDED.base_fare,
                            tax_surcharge = EXCLUDED.tax_surcharge,
                            total_fare = EXCLUDED.total_fare,
                            arrival_time = EXCLUDED.arrival_time,
                            duration = EXCLUDED.duration,
                            stops = EXCLUDED.stops,
                            season = EXCLUDED.season,
                            is_peak_season = EXCLUDED.is_peak_season,
                            record_hash = EXCLUDED.record_hash,
                            last_updated_date = EXCLUDED.last_updated_date,
                            version_number = {table_name}.version_number + 1,
                            is_active = EXCLUDED.is_active
                        RETURNING (xmax = 0) AS inserted
                    )
                    SELECT 
                        COUNT(*) FILTER (WHERE inserted) as inserts,
                        COUNT(*) FILTER (WHERE NOT inserted) as updates
                    FROM upsert
                """
                
                with self.postgres_engine.begin() as conn:
                    result = conn.execute(text(upsert_query)).fetchone()
                    batch_inserts = result[0] if result else 0
                    batch_updates = result[1] if result else 0
                    
                    rows_inserted += batch_inserts
                    rows_updated += batch_updates
                    
                    logger.info(f"Batch {i//batch_size + 1}: {batch_inserts} inserted, {batch_updates} updated")
                    
                    # Drop temp table
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            
            logger.info(f"Incremental save completed: {rows_inserted} inserted, {rows_updated} updated")
            
            return rows_inserted, rows_updated
            
        except Exception as e:
            logger.error(f"Error in incremental save: {str(e)}")
            raise TransformationError(f"Error in incremental save: {str(e)}")
    
    
    
    def execute_transformation(self) -> Dict:
        """
        Main execution method for data transformation
        Supports both full refresh and incremental patterns
        
        Returns:
            dict: Transformation results
        """
        try:
            logger.info("Starting data transformation process")
            
            # Determine load strategy
            use_incremental = pipeline_config.USE_INCREMENTAL_LOAD
            
            if use_incremental:
                logger.info("Using INCREMENTAL transformation mode")
                # Load only new/changed records
                df = self.load_staging_data_incremental()
            else:
                logger.info("Using FULL REFRESH transformation mode")
                # Load all records
                df = self.load_staging_data()
            
            initial_count = len(df)
            
            if initial_count == 0:
                logger.info("No records to transform")
                return {
                    'status': 'SUCCESS',
                    'load_mode': 'INCREMENTAL' if use_incremental else 'FULL_REFRESH',
                    'initial_records': 0,
                    'final_records': 0,
                    'records_saved': 0,
                    'records_inserted': 0,
                    'records_updated': 0
                }
            
            # Transform data
            df = self.calculate_total_fare(df)
            df = self.add_seasonal_features(df)
            df = self.clean_and_standardize(df)
            
            final_count = len(df)
            
            # Save to analytics database
            if use_incremental:
                rows_inserted, rows_updated = self.save_to_analytics_db_incremental(df)
                records_saved = rows_inserted + rows_updated
            else:
                records_saved = self.save_to_analytics_db(df)
                rows_inserted = records_saved
                rows_updated = 0
            
            return {
                'status': 'SUCCESS',
                'load_mode': 'INCREMENTAL' if use_incremental else 'FULL_REFRESH',
                'initial_records': initial_count,
                'final_records': final_count,
                'records_saved': records_saved,
                'records_inserted': rows_inserted,
                'records_updated': rows_updated,
                'records_removed': initial_count - final_count
            }
            
        except Exception as e:
            logger.error(f"Transformation execution failed: {str(e)}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
        finally:
            self.mysql_engine.dispose()
            self.postgres_engine.dispose()




def main():
    """Main entry point"""
    transformer = DataTransformer()
    result = transformer.execute_transformation()
    
    if result['status'] == 'SUCCESS':
        logger.info(f"✓ Transformation completed: {result}")
        return 0
    else:
        logger.error(f"✗ Transformation failed: {result}")
        return 1


if __name__ == '__main__':
    exit(main())