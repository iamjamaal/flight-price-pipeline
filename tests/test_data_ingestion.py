"""
Unit tests for Data Ingestion module
"""
import unittest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
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


if __name__ == '__main__':
    unittest.main()