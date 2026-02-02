"""
KPI Computation Module
Calculates and stores key performance indicators
Filters only active records for accurate metrics
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Dict
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config, pipeline_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KPIComputationError(Exception):
    """Custom exception for KPI computation errors"""
    pass


class KPIComputer:
    """Computes and stores KPI metrics"""
    
    def __init__(self):
        self.postgres_engine = create_engine(db_config.postgres_connection_string, pool_pre_ping=True)
        logger.info("KPI Computer initialized")
    
    def load_analytics_data(self) -> pd.DataFrame:
        """Load active records only from analytics table for KPI computation"""
        try:
            # Only compute KPIs on active records
            if pipeline_config.USE_INCREMENTAL_LOAD:
                query = """
                    SELECT * FROM flights_analytics 
                    WHERE is_active = TRUE
                """
                logger.info("Loading active records only for KPI computation (incremental mode)")
            else:
                query = "SELECT * FROM flights_analytics"
                logger.info("Loading all records for KPI computation (full refresh mode)")
            
            df = pd.read_sql(query, self.postgres_engine)
            logger.info(f"Loaded {len(df)} records for KPI computation")
            return df
        except Exception as e:
            logger.error(f"Error loading analytics data: {str(e)}")
            raise KPIComputationError(f"Error loading analytics data: {str(e)}")
    
    
    
    def compute_average_fare_by_airline(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 1: Compute average fare metrics by airline
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: KPI results
        """
        try:
            logger.info("Computing average fare by airline...")
            
            kpi_df = df.groupby('airline').agg({
                'base_fare': ['mean', 'min', 'max'],
                'tax_surcharge': 'mean',
                'total_fare': ['mean', 'min', 'max'],
                'airline': 'count'
            }).reset_index()
            
            # Flatten multi-level columns
            kpi_df.columns = ['airline', 'avg_base_fare', 'min_base_fare', 'max_base_fare',
                             'avg_tax_surcharge', 'avg_total_fare', 'min_total_fare', 
                             'max_total_fare', 'booking_count']
            
            # Round to 2 decimal places
            numeric_columns = kpi_df.select_dtypes(include=['float64']).columns
            kpi_df[numeric_columns] = kpi_df[numeric_columns].round(2)
            
            logger.info(f"Computed metrics for {len(kpi_df)} airlines")
            
            return kpi_df
            
        except Exception as e:
            logger.error(f"Error computing average fare by airline: {str(e)}")
            raise KPIComputationError(f"Error computing average fare by airline: {str(e)}")
    
    
    
    def compute_seasonal_fare_variation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 2: Compute seasonal fare variation
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: KPI results
        """
        try:
            logger.info("Computing seasonal fare variation...")
            
            kpi_df = df.groupby(['season', 'is_peak_season']).agg({
                'total_fare': ['mean', 'median', 'min', 'max', 'std'],
                'season': 'count'
            }).reset_index()
            
            # Flatten columns
            kpi_df.columns = ['season', 'is_peak_season', 'avg_fare', 'median_fare',
                             'min_fare', 'max_fare', 'std_dev_fare', 'booking_count']
            
            # Round to 2 decimal places
            numeric_columns = kpi_df.select_dtypes(include=['float64']).columns
            kpi_df[numeric_columns] = kpi_df[numeric_columns].round(2)
            
            # Fill NaN std_dev with 0 (happens when only 1 record in group)
            kpi_df['std_dev_fare'] = kpi_df['std_dev_fare'].fillna(0)
            
            logger.info(f"Computed metrics for {len(kpi_df)} season combinations")
            
            return kpi_df
            
        except Exception as e:
            logger.error(f"Error computing seasonal fare variation: {str(e)}")
            raise KPIComputationError(f"Error computing seasonal fare variation: {str(e)}")
    
    
    
    def compute_popular_routes(self, df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        KPI 3: Identify most popular routes
        
        Args:
            df: Input DataFrame
            top_n: Number of top routes to return
            
        Returns:
            pd.DataFrame: KPI results
        """
        try:
            logger.info(f"Computing top {top_n} popular routes...")
            
            kpi_df = df.groupby(['source', 'destination']).agg({
                'source': 'count',
                'total_fare': ['mean', 'min', 'max']
            }).reset_index()
            
            # Flatten columns
            kpi_df.columns = ['source', 'destination', 'booking_count', 
                             'avg_fare', 'min_fare', 'max_fare']
            
            # Create route string
            kpi_df['route'] = kpi_df['source'] + ' -> ' + kpi_df['destination']
            
            # Sort by booking count and assign ranks
            kpi_df = kpi_df.sort_values('booking_count', ascending=False)
            kpi_df['route_rank'] = range(1, len(kpi_df) + 1)
            
            # Get top N routes
            kpi_df = kpi_df.head(top_n)
            
            # Round to 2 decimal places
            numeric_columns = ['avg_fare', 'min_fare', 'max_fare']
            kpi_df[numeric_columns] = kpi_df[numeric_columns].round(2)
            
            logger.info(f"Identified top {len(kpi_df)} popular routes")
            
            return kpi_df
            
        except Exception as e:
            logger.error(f"Error computing popular routes: {str(e)}")
            raise KPIComputationError(f"Error computing popular routes: {str(e)}")
    
    
    
    def compute_booking_count_by_airline(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 4: Compute booking count metrics by airline
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: KPI results
        """
        try:
            logger.info("Computing booking count by airline...")
            
            # Overall bookings
            total_bookings = df.groupby('airline').size().reset_index(name='total_bookings')
            
            # Peak season bookings
            peak_bookings = df[df['is_peak_season'] == True].groupby('airline').size().reset_index(name='peak_season_bookings')
            
            # Off season bookings
            off_bookings = df[df['is_peak_season'] == False].groupby('airline').size().reset_index(name='off_season_bookings')
            
            # Merge all metrics
            kpi_df = total_bookings.merge(peak_bookings, on='airline', how='left')
            kpi_df = kpi_df.merge(off_bookings, on='airline', how='left')
            
            # Fill NaN with 0
            kpi_df = kpi_df.fillna(0)
            
            # Calculate market share percentage
            total_market = kpi_df['total_bookings'].sum()
            kpi_df['market_share_percentage'] = ((kpi_df['total_bookings'] / total_market) * 100).round(2)
            
            # Convert booking counts to integers
            booking_columns = ['total_bookings', 'peak_season_bookings', 'off_season_bookings']
            kpi_df[booking_columns] = kpi_df[booking_columns].astype(int)
            
            logger.info(f"Computed booking metrics for {len(kpi_df)} airlines")
            
            return kpi_df
            
        except Exception as e:
            logger.error(f"Error computing booking count by airline: {str(e)}")
            raise KPIComputationError(f"Error computing booking count by airline: {str(e)}")
    
    
    
    def save_kpi_to_db(self, kpi_df: pd.DataFrame, table_name: str) -> int:
        """
        Save KPI DataFrame to database
        
        Args:
            kpi_df: KPI DataFrame
            table_name: Target table name
            
        Returns:
            int: Number of records saved
        """
        try:
            # Clear existing data
            with self.postgres_engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))
            
            # Save new data
            kpi_df.to_sql(
                name=table_name,
                con=self.postgres_engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Saved {len(kpi_df)} records to {table_name}")
            
            return len(kpi_df)
            
        except Exception as e:
            logger.error(f"Error saving KPI to database: {str(e)}")
            raise KPIComputationError(f"Error saving KPI to database: {str(e)}")
    
    
    
    def execute_kpi_computation(self) -> Dict:
        """
        Main execution method for KPI computation
        
        Returns:
            dict: Computation results
        """
        try:
            logger.info("Starting KPI computation process")
            
            # Load data
            df = self.load_analytics_data()
            
            kpi_results = {}
            
            # Compute and save KPI 1: Average Fare by Airline
            kpi1 = self.compute_average_fare_by_airline(df)
            records_saved = self.save_kpi_to_db(kpi1, 'kpi_average_fare_by_airline')
            kpi_results['average_fare_by_airline'] = records_saved
            
            # Compute and save KPI 2: Seasonal Fare Variation
            kpi2 = self.compute_seasonal_fare_variation(df)
            records_saved = self.save_kpi_to_db(kpi2, 'kpi_seasonal_fare_variation')
            kpi_results['seasonal_fare_variation'] = records_saved
            
            # Compute and save KPI 3: Popular Routes
            kpi3 = self.compute_popular_routes(df, top_n=20)
            records_saved = self.save_kpi_to_db(kpi3, 'kpi_popular_routes')
            kpi_results['popular_routes'] = records_saved
            
            # Compute and save KPI 4: Booking Count by Airline
            kpi4 = self.compute_booking_count_by_airline(df)
            records_saved = self.save_kpi_to_db(kpi4, 'kpi_booking_count_by_airline')
            kpi_results['booking_count_by_airline'] = records_saved
            
            logger.info("All KPIs computed and saved successfully")
            
            return {
                'status': 'SUCCESS',
                'kpis_computed': len(kpi_results),
                'details': kpi_results
            }
            
        except Exception as e:
            logger.error(f"KPI computation failed: {str(e)}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
        finally:
            self.postgres_engine.dispose()



def main():
    """Main entry point"""
    kpi_computer = KPIComputer()
    result = kpi_computer.execute_kpi_computation()
    
    if result['status'] == 'SUCCESS':
        logger.info(f"✓ KPI computation completed: {result}")
        return 0
    else:
        logger.error(f"✗ KPI computation failed: {result}")
        return 1


if __name__ == '__main__':
    exit(main())