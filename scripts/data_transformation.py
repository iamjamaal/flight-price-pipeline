"""
Data Transformation Module
Handles data transformation and seasonal classification
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Dict
from datetime import datetime
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
        """Load validated data from staging"""
        try:
            query = "SELECT * FROM staging_flights"
            df = pd.read_sql(query, self.mysql_engine)
            logger.info(f"Loaded {len(df)} records from staging for transformation")
            return df
        except Exception as e:
            logger.error(f"Error loading staging data: {str(e)}")
            raise TransformationError(f"Error loading staging data: {str(e)}")
    
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
            
            # Clear existing data
            with self.postgres_engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                conn.commit()
                logger.info(f"Truncated {table_name} table")
            
            # Save data in batches
            batch_size = pipeline_config.BATCH_SIZE
            total_saved = 0
            
            for i in range(0, len(df_to_save), batch_size):
                batch_df = df_to_save.iloc[i:i+batch_size]
                batch_df.to_sql(
                    name=table_name,
                    con=self.postgres_engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                total_saved += len(batch_df)
                logger.info(f"Saved batch {i//batch_size + 1}: {len(batch_df)} records")
            
            logger.info(f"Total records saved to {table_name}: {total_saved}")
            
            return total_saved
            
        except Exception as e:
            logger.error(f"Error saving to analytics database: {str(e)}")
            raise TransformationError(f"Error saving to analytics database: {str(e)}")
    
    def execute_transformation(self) -> Dict:
        """
        Main execution method for data transformation
        
        Returns:
            dict: Transformation results
        """
        try:
            logger.info("Starting data transformation process")
            
            # Load data
            df = self.load_staging_data()
            initial_count = len(df)
            
            # Transform data
            df = self.calculate_total_fare(df)
            df = self.add_seasonal_features(df)
            df = self.clean_and_standardize(df)
            
            final_count = len(df)
            
            # Save to analytics database
            records_saved = self.save_to_analytics_db(df)
            
            return {
                'status': 'SUCCESS',
                'initial_records': initial_count,
                'final_records': final_count,
                'records_saved': records_saved,
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