"""
Integration tests for the entire pipeline
"""
import unittest
from sqlalchemy import create_engine, text
import sys
from datetime import datetime
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config, pipeline_config


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests for the full pipeline"""
    
    def setUp(self):
        """Set up test databases"""
        self.mysql_engine = create_engine(db_config.mysql_connection_string)
        self.postgres_engine = create_engine(db_config.postgres_connection_string)
    
    def test_mysql_connection(self):
        """Test MySQL database connection"""
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertEqual(result.scalar(), 1)
    
    def test_postgres_connection(self):
        """Test PostgreSQL database connection"""
        with self.postgres_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertEqual(result.scalar(), 1)
    
    def test_staging_table_exists(self):
        """Test that staging table exists"""
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'staging_db' 
                AND table_name = 'staging_flights'
            """))
            self.assertEqual(result.scalar(), 1)
    
    def test_staging_incremental_columns(self):
        """Test that staging table has incremental loading columns"""
        expected_columns = ['record_hash', 'is_active', 'ingestion_timestamp']
        
        with self.mysql_engine.connect() as conn:
            for col in expected_columns:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'staging_db' 
                    AND table_name = 'staging_flights'
                    AND column_name = '{col}'
                """))
                self.assertEqual(result.scalar(), 1, f"Column {col} should exist in staging_flights")
    
    def test_analytics_tables_exist(self):
        """Test that all analytics tables exist"""
        expected_tables = [
            'flights_analytics',
            'kpi_average_fare_by_airline',
            'kpi_seasonal_fare_variation',
            'kpi_popular_routes',
            'kpi_booking_count_by_airline',
            'pipeline_execution_log',
            'flights_analytics_history'
        ]
        
        with self.postgres_engine.connect() as conn:
            for table in expected_tables:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                """))
                self.assertEqual(result.scalar(), 1, f"Table {table} should exist")
    
    def test_analytics_incremental_columns(self):
        """Test that analytics table has versioning columns"""
        expected_columns = ['record_hash', 'first_seen_date', 'last_updated_date', 
                          'version_number', 'is_active']
        
        with self.postgres_engine.connect() as conn:
            for col in expected_columns:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'flights_analytics'
                    AND column_name = '{col}'
                """))
                self.assertEqual(result.scalar(), 1, f"Column {col} should exist in flights_analytics")
    
    def test_processing_mode_tracking(self):
        """Test that pipeline execution log tracks processing mode"""
        tracking_columns = ['processing_mode', 'records_inserted', 'records_updated']
        
        with self.postgres_engine.connect() as conn:
            for col in tracking_columns:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'pipeline_execution_log'
                    AND column_name = '{col}'
                """))
                self.assertEqual(result.scalar(), 1, f"Column {col} should exist in pipeline_execution_log")
    
    def test_monitoring_views_exist(self):
        """Test that monitoring views exist"""
        expected_views = [
            'vw_task_performance',
            'vw_pipeline_execution_summary',
            'vw_incremental_load_stats',
            'vw_record_change_history'
        ]
        
        with self.postgres_engine.connect() as conn:
            for view in expected_views:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = '{view}'
                """))
                self.assertEqual(result.scalar(), 1, f"View {view} should exist")
    
    def test_incremental_config(self):
        """Test incremental loading configuration"""
        self.assertIsInstance(pipeline_config.USE_INCREMENTAL_LOAD, bool)
        self.assertIsInstance(pipeline_config.FULL_REFRESH_DAY, int)
        self.assertIn(pipeline_config.FULL_REFRESH_DAY, range(7))  # 0-6 for days of week
        self.assertIn(pipeline_config.HASH_ALGORITHM, ['md5', 'sha256'])
    
    def test_data_quality_log_exists(self):
        """Test that data quality log table exists in MySQL"""
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'staging_db' 
                AND table_name = 'data_quality_log'
            """))
            self.assertEqual(result.scalar(), 1)
    
    def tearDown(self):
        """Clean up connections"""
        self.mysql_engine.dispose()
        self.postgres_engine.dispose()


if __name__ == '__main__':
    unittest.main()