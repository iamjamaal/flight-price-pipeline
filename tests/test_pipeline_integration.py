"""
Integration tests for the entire pipeline
"""
import unittest
from sqlalchemy import create_engine, text
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config


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
    
    def test_analytics_tables_exist(self):
        """Test that all analytics tables exist"""
        expected_tables = [
            'flights_analytics',
            'kpi_average_fare_by_airline',
            'kpi_seasonal_fare_variation',
            'kpi_popular_routes',
            'kpi_booking_count_by_airline'
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
    
    def tearDown(self):
        """Clean up connections"""
        self.mysql_engine.dispose()
        self.postgres_engine.dispose()


if __name__ == '__main__':
    unittest.main()