"""
Test runner for flight price pipeline
Run this script to execute all test suites
"""
import sys
import subprocess

def run_command(cmd, description):
    """Run a command and report results"""
    print(f"\n{'='*80}")
    print(f"Running: {description}")
    print(f"{'='*80}")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        print(result.stdout)
        if result.stderr and "WARNING" not in result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(" Test timed out")
        return False
    except Exception as e:
        print(f" Error running test: {e}")
        return False

def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("FLIGHT PRICE PIPELINE - TEST SUITE")
    print("="*80)
    
    results = {}
    
    # Test 1: Monitoring module functionality
    results['Monitoring Module'] = run_command(
        'docker exec airflow-webserver python -c "import sys; sys.path.append(\'/opt/airflow/scripts\'); from monitoring import PipelineMonitor; m = PipelineMonitor(); print(\'✓ Monitoring module loaded\'); metrics = m.get_performance_metrics(); print(f\'✓ Tasks tracked: {len(metrics.get(\\\"tasks\\\", []))}\'); print(\'✅ All monitoring tests PASSED\')"',
        "Monitoring Module Tests"
    )
    
    # Test 2: Database connections
    results['PostgreSQL Connection'] = run_command(
        'docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = \'public\' AND table_type = \'BASE TABLE\';"',
        "PostgreSQL Connection & Tables"
    )
    
    results['MySQL Connection'] = run_command(
        'docker exec mysql-staging mysql -u staging_user -pstaging_pass staging_db -e "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = \'staging_db\';" 2>nul',
        "MySQL Connection & Tables"
    )
    
    # Test 3: Incremental loading columns
    results['Incremental Columns'] = run_command(
        'docker exec mysql-staging mysql -u staging_user -pstaging_pass staging_db -e "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = \'staging_db\' AND TABLE_NAME = \'staging_flights\' AND COLUMN_NAME IN (\'record_hash\', \'is_active\', \'ingestion_timestamp\');" 2>nul',
        "Incremental Loading Columns Check"
    )
    
    # Test 4: Monitoring views
    results['Monitoring Views'] = run_command(
        'docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "SELECT table_name FROM information_schema.views WHERE table_schema = \'public\' ORDER BY table_name;"',
        "Monitoring Views Existence"
    )
    
    # Test 5: Processing mode tracking
    results['Processing Mode Tracking'] = run_command(
        'docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "SELECT DISTINCT processing_mode FROM pipeline_execution_log WHERE processing_mode IS NOT NULL LIMIT 5;"',
        "Processing Mode Tracking"
    )
    
    # Test 6: Task performance with WARNING status
    results['Task Performance Metrics'] = run_command(
        'docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "SELECT task_id, success_count, execution_count, success_rate FROM vw_task_performance WHERE task_id = \'data_validation\';"',
        "Task Performance (WARNING Status Handling)"
    )
    
    # Print summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, passed_flag in results.items():
        status = "PASSED" if passed_flag else "FAILED"
        print(f"{test_name:.<50} {status}")
    
    print(f"\n{passed}/{total} tests passed ({(passed/total)*100:.1f}%)")
    
    if passed == total:
        print("\n All tests PASSED!")
        return 0
    else:
        print(f"\n {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
