"""
Unit tests for Data Ingestion module
"""
import unittest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
import hashlib
sys.path.append('/opt/airflow/scripts')

from data_ingestion import DataIngestion, DataIngestionError


class TestDataIngestion(unittest.TestCase):
    """Test cases for DataIngestion class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.ingestion = DataIngestion()
    
    @patch('data_ingestion.os.path.exists')
    @patch('data_ingestion.os.access')
    def test_validate_csv_file_success(self, mock_access, mock_exists):
        """Test successful CSV validation"""
        mock_exists.return_value = True
        mock_access.return_value = True
        
        result = self.ingestion.validate_csv_file('/test/path.csv')
        self.assertTrue(result)
    
    @patch('data_ingestion.os.path.exists')
    def test_validate_csv_file_not_found(self, mock_exists):
        """Test CSV validation when file doesn't exist"""
        mock_exists.return_value = False
        
        with self.assertRaises(DataIngestionError):
            self.ingestion.validate_csv_file('/test/missing.csv')
    
    def test_standardize_column_names(self):
        """Test column name standardization"""
        df = pd.DataFrame({
            'Airline': ['Test Air'],
            'Source': ['City A'],
            'Destination': ['City B'],
            'Base Fare': [100],
            'Tax & Surcharge': [20],
            'Total Fare': [120]
        })
        
        result = self.ingestion.standardize_column_names(df)
        
        expected_columns = ['airline', 'source', 'destination', 'base_fare', 'tax_surcharge', 'total_fare']
        self.assertListEqual(result.columns.tolist(), expected_columns)
    
    def test_clean_and_prepare_data(self):
        """Test data cleaning"""
        df = pd.DataFrame({
            'airline': ['  Test Air  '],
            'base_fare': ['100.50'],
            'tax_surcharge': ['20.30']
        })
        
        result = self.ingestion.clean_and_prepare_data(df)
        
        self.assertEqual(result['airline'].iloc[0], 'Test Air')
        self.assertEqual(result['base_fare'].dtype, 'float64')
    
    def test_generate_record_hash(self):
        """Test hash generation for records"""
        df = pd.DataFrame({
            'airline': ['Test Air'],
            'source': ['City A'],
            'destination': ['City B'],
            'date_of_journey': ['2024-01-01'],
            'departure_time': ['10:00'],
            'base_fare': [100.0],
            'tax_surcharge': [20.0],
            'total_fare': [120.0]
        })
        
        result = self.ingestion.generate_record_hash(df)
        
        # Check hash column exists
        self.assertIn('record_hash', result.columns)
        
        # Check hash is not null
        self.assertFalse(result['record_hash'].isnull().any())
        
        # Check hash format (MD5 produces 32 character hex string)
        self.assertEqual(len(result['record_hash'].iloc[0]), 32)
        
        # Check hash is consistent for same data
        result2 = self.ingestion.generate_record_hash(df)
        self.assertEqual(result['record_hash'].iloc[0], result2['record_hash'].iloc[0])
    
    def test_incremental_columns_added(self):
        """Test that incremental loading columns are added"""
        df = pd.DataFrame({
            'airline': ['Test Air'],
            'base_fare': [100.0]
        })
        
        result = self.ingestion.add_incremental_columns(df)
        
        # Check all incremental columns exist
        expected_columns = ['record_hash', 'is_active', 'ingestion_timestamp']
        for col in expected_columns:
            self.assertIn(col, result.columns)
        
        # Check default values
        self.assertTrue(result['is_active'].iloc[0])
        self.assertIsNotNone(result['ingestion_timestamp'].iloc[0])


if __name__ == '__main__':
    unittest.main()