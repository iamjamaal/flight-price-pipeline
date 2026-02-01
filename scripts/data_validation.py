"""
Data Validation Module
Implements comprehensive data quality checks
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Dict, List, Tuple
from datetime import datetime
import sys
sys.path.append('/opt/airflow')

from dags.config.pipeline_config import db_config, pipeline_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


class DataValidator:
    """Performs comprehensive data validation checks"""
    
    def __init__(self):
        self.mysql_engine = create_engine(
            db_config.mysql_connection_string,
            pool_pre_ping=True
        )
        self.validation_results = []
        logger.info("Data Validator initialized")
    
    def load_staging_data(self) -> pd.DataFrame:
        """Load data from staging table"""
        try:
            query = "SELECT * FROM staging_flights"
            df = pd.read_sql(query, self.mysql_engine)
            logger.info(f"Loaded {len(df)} records from staging")
            return df
        except Exception as e:
            logger.error(f"Error loading staging data: {str(e)}")
            raise ValidationError(f"Error loading staging data: {str(e)}")
    
    def check_required_columns(self, df: pd.DataFrame) -> Dict:
        """
        Validate that all required columns are present
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation result
        """
        try:
            missing_columns = []
            for col in pipeline_config.REQUIRED_COLUMNS:
                # Check with case-insensitive comparison
                if not any(df_col.lower() == col.lower() for df_col in df.columns):
                    missing_columns.append(col)
            
            status = 'PASSED' if len(missing_columns) == 0 else 'FAILED'
            
            result = {
                'check_name': 'REQUIRED_COLUMNS_CHECK',
                'status': status,
                'records_checked': len(df),
                'records_failed': 0 if status == 'PASSED' else 1,
                'error_details': f"Missing columns: {missing_columns}" if missing_columns else None
            }
            
            self.validation_results.append(result)
            logger.info(f"Required columns check: {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"Required columns check failed: {str(e)}")
            raise ValidationError(str(e))
    
    def check_null_values(self, df: pd.DataFrame) -> Dict:
        """
        Check for null values in required columns
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation result
        """
        try:
            required_non_null = ['airline', 'source', 'destination']
            null_counts = {}
            
            for col in required_non_null:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    null_percentage = (null_count / len(df)) * 100
                    if null_count > 0:
                        null_counts[col] = {'count': null_count, 'percentage': null_percentage}
            
            records_failed = sum(v['count'] for v in null_counts.values())
            status = 'FAILED' if records_failed > 0 else 'PASSED'
            
            result = {
                'check_name': 'NULL_VALUES_CHECK',
                'status': status,
                'records_checked': len(df),
                'records_failed': records_failed,
                'error_details': str(null_counts) if null_counts else None
            }
            
            self.validation_results.append(result)
            logger.info(f"Null values check: {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"Null values check failed: {str(e)}")
            raise ValidationError(str(e))
    
    def check_data_types(self, df: pd.DataFrame) -> Dict:
        """
        Validate data types for numeric and string columns
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation result
        """
        try:
            type_errors = []
            
            # Check numeric columns
            numeric_columns = ['base_fare', 'tax_surcharge', 'total_fare']
            for col in numeric_columns:
                if col in df.columns:
                    non_numeric = df[~pd.to_numeric(df[col], errors='coerce').notna()][col]
                    if len(non_numeric) > 0:
                        type_errors.append(f"{col}: {len(non_numeric)} non-numeric values")
            
            # Check string columns
            string_columns = ['airline', 'source', 'destination']
            for col in string_columns:
                if col in df.columns:
                    if df[col].dtype != 'object':
                        type_errors.append(f"{col}: Expected string type")
            
            records_failed = len(type_errors)
            status = 'PASSED' if records_failed == 0 else 'FAILED'
            
            result = {
                'check_name': 'DATA_TYPE_CHECK',
                'status': status,
                'records_checked': len(df),
                'records_failed': records_failed,
                'error_details': '; '.join(type_errors) if type_errors else None
            }
            
            self.validation_results.append(result)
            logger.info(f"Data type check: {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"Data type check failed: {str(e)}")
            raise ValidationError(str(e))
    
    def check_fare_consistency(self, df: pd.DataFrame) -> Dict:
        """
        Validate fare values are consistent and reasonable
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation result
        """
        try:
            errors = []
            
            # Check for negative fares
            if 'base_fare' in df.columns:
                negative_base = (df['base_fare'] < 0).sum()
                if negative_base > 0:
                    errors.append(f"{negative_base} negative base fares")
            
            if 'tax_surcharge' in df.columns:
                negative_tax = (df['tax_surcharge'] < 0).sum()
                if negative_tax > 0:
                    errors.append(f"{negative_tax} negative tax/surcharge")
            
            if 'total_fare' in df.columns:
                negative_total = (df['total_fare'] < 0).sum()
                if negative_total > 0:
                    errors.append(f"{negative_total} negative total fares")
            
            # Check if total_fare = base_fare + tax_surcharge (with tolerance)
            if all(col in df.columns for col in ['base_fare', 'tax_surcharge', 'total_fare']):
                df_numeric = df[['base_fare', 'tax_surcharge', 'total_fare']].apply(pd.to_numeric, errors='coerce')
                calculated_total = df_numeric['base_fare'] + df_numeric['tax_surcharge']
                fare_mismatch = ((df_numeric['total_fare'] - calculated_total).abs() > 0.01).sum()
                if fare_mismatch > 0:
                    errors.append(f"{fare_mismatch} fare calculation mismatches")
            
            # Check for unreasonably high fares
            if 'total_fare' in df.columns:
                df_numeric = df['total_fare'].apply(pd.to_numeric, errors='coerce')
                high_fares = (df_numeric > pipeline_config.MAX_FARE_VALUE).sum()
                if high_fares > 0:
                    errors.append(f"{high_fares} unreasonably high fares (>{pipeline_config.MAX_FARE_VALUE})")
            
            records_failed = len(errors)
            status = 'PASSED' if records_failed == 0 else 'WARNING' if records_failed < 10 else 'FAILED'
            
            result = {
                'check_name': 'FARE_CONSISTENCY_CHECK',
                'status': status,
                'records_checked': len(df),
                'records_failed': records_failed,
                'error_details': '; '.join(errors) if errors else None
            }
            
            self.validation_results.append(result)
            logger.info(f"Fare consistency check: {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"Fare consistency check failed: {str(e)}")
            raise ValidationError(str(e))
    
    def check_city_names(self, df: pd.DataFrame) -> Dict:
        """
        Validate city names are not empty and contain valid characters
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation result
        """
        try:
            errors = []
            
            city_columns = ['source', 'destination']
            for col in city_columns:
                if col in df.columns:
                    # Check for empty strings
                    empty_cities = (df[col].str.strip() == '').sum()
                    if empty_cities > 0:
                        errors.append(f"{col}: {empty_cities} empty city names")
                    
                    # Check for invalid characters (numbers only)
                    invalid_cities = df[col].str.match(r'^\d+$').sum()
                    if invalid_cities > 0:
                        errors.append(f"{col}: {invalid_cities} invalid city names (numbers only)")
            
            records_failed = len(errors)
            status = 'PASSED' if records_failed == 0 else 'FAILED'
            
            result = {
                'check_name': 'CITY_NAME_CHECK',
                'status': status,
                'records_checked': len(df),
                'records_failed': records_failed,
                'error_details': '; '.join(errors) if errors else None
            }
            
            self.validation_results.append(result)
            logger.info(f"City name check: {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"City name check failed: {str(e)}")
            raise ValidationError(str(e))
    
    def check_duplicate_records(self, df: pd.DataFrame) -> Dict:
        """
        Check for duplicate records
        
        Args:
            df: DataFrame to validate
            
        Returns:
            dict: Validation result
        """
        try:
            key_columns = ['airline', 'source', 'destination', 'date_of_journey', 'departure_time']
            available_columns = [col for col in key_columns if col in df.columns]
            
            if available_columns:
                duplicates = df.duplicated(subset=available_columns, keep='first').sum()
                status = 'PASSED' if duplicates == 0 else 'WARNING'
            else:
                duplicates = 0
                status = 'PASSED'
            
            result = {
                'check_name': 'DUPLICATE_RECORDS_CHECK',
                'status': status,
                'records_checked': len(df),
                'records_failed': duplicates,
                'error_details': f"Found {duplicates} duplicate records" if duplicates > 0 else None
            }
            
            self.validation_results.append(result)
            logger.info(f"Duplicate records check: {status}")
            
            return result
            
        except Exception as e:
            logger.error(f"Duplicate records check failed: {str(e)}")
            raise ValidationError(str(e))
    
    def log_validation_results(self):
        """Log all validation results to database"""
        try:
            with self.mysql_engine.connect() as conn:
                for result in self.validation_results:
                    query = text("""
                        INSERT INTO data_quality_log 
                        (check_name, check_status, records_checked, records_failed, error_details)
                        VALUES (:check_name, :status, :records_checked, :records_failed, :error_details)
                    """)
                    conn.execute(query, result)
                conn.commit()
                
            logger.info(f"Logged {len(self.validation_results)} validation results")
        except Exception as e:
            logger.warning(f"Failed to log validation results: {str(e)}")
    
    def execute_validation(self) -> Dict:
        """
        Main execution method for data validation
        
        Returns:
            dict: Overall validation results
        """
        try:
            logger.info("Starting data validation process")
            
            # Load data
            df = self.load_staging_data()
            
            # Run all validation checks
            self.check_required_columns(df)
            self.check_null_values(df)
            self.check_data_types(df)
            self.check_fare_consistency(df)
            self.check_city_names(df)
            self.check_duplicate_records(df)
            
            # Log results
            self.log_validation_results()
            
            # Determine overall status
            failed_checks = [r for r in self.validation_results if r['status'] == 'FAILED']
            warning_checks = [r for r in self.validation_results if r['status'] == 'WARNING']
            
            overall_status = 'FAILED' if failed_checks else 'WARNING' if warning_checks else 'PASSED'
            
            return {
                'status': overall_status,
                'total_checks': len(self.validation_results),
                'passed': len([r for r in self.validation_results if r['status'] == 'PASSED']),
                'warnings': len(warning_checks),
                'failed': len(failed_checks),
                'details': self.validation_results
            }
            
        except Exception as e:
            logger.error(f"Validation execution failed: {str(e)}")
            return {
                'status': 'ERROR',
                'error': str(e)
            }
        finally:
            self.mysql_engine.dispose()


def main():
    """Main entry point"""
    validator = DataValidator()
    result = validator.execute_validation()
    
    logger.info(f"Validation completed: {result['status']}")
    logger.info(f"Passed: {result.get('passed', 0)}, Warnings: {result.get('warnings', 0)}, Failed: {result.get('failed', 0)}")
    
    if result['status'] == 'FAILED':
        return 1
    return 0


if __name__ == '__main__':
    exit(main())