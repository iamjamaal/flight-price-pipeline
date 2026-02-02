"""
Test monitoring functionality
Run this to verify monitoring system is working
"""
import sys
sys.path.append('/opt/airflow/scripts')

from monitoring import PipelineMonitor


def test_health_check():
    """Test health status check"""
    print("\n" + "="*80)
    print("TEST: Health Check")
    print("="*80)
    
    monitor = PipelineMonitor()
    health = monitor.get_pipeline_health_status()
    
    print(f"\nOverall Status: {health['overall_status']}")
    print(f"Timestamp: {health['timestamp']}")
    
    for component, status in health.get('components', {}).items():
        print(f"\n{component}:")
        for key, value in status.items():
            print(f"  {key}: {value}")
    
    assert health['overall_status'] in ['HEALTHY', 'WARNING', 'UNHEALTHY', 'ERROR']
    print("\n✅ Health check test PASSED")


def test_performance_metrics():
    """Test performance metrics collection"""
    print("\n" + "="*80)
    print("TEST: Performance Metrics")
    print("="*80)
    
    monitor = PipelineMonitor()
    metrics = monitor.get_performance_metrics()
    
    print(f"\nPeriod: {metrics.get('period', 'N/A')}")
    print(f"Tasks tracked: {len(metrics.get('tasks', []))}")
    
    for task in metrics.get('tasks', []):
        print(f"\n{task['task_id']}:")
        print(f"  Executions: {task['execution_count']}")
        print(f"  Avg time: {task['avg_execution_time']}s")
        print(f"  Success rate: {task['success_rate']}%")
    
    print("\n✅ Performance metrics test PASSED")


def test_data_quality():
    """Test data quality assessment"""
    print("\n" + "="*80)
    print("TEST: Data Quality")
    print("="*80)
    
    monitor = PipelineMonitor()
    quality = monitor.get_data_quality_metrics()
    
    metrics = quality.get('metrics', {})
    
    if 'completeness' in metrics:
        comp = metrics['completeness']
        print(f"\nCompleteness:")
        print(f"  Total records: {comp['total_records']:,}")
        print(f"  Date coverage: {comp['date_coverage']}%")
        print(f"  Airline coverage: {comp['airline_coverage']}%")
        print(f"  Fare coverage: {comp['fare_coverage']}%")
    
    if 'fare_statistics' in metrics:
        fares = metrics['fare_statistics']
        print(f"\nFare Statistics:")
        print(f"  Average: ₹{fares['average']:,.2f}")
        print(f"  Min: ₹{fares['minimum']:,.2f}")
        print(f"  Max: ₹{fares['maximum']:,.2f}")
    
    if 'validation_history' in metrics:
        print(f"\nValidation History:")
        for check in metrics['validation_history']:
            print(f"  {check['check_name']}: {check['pass_rate']}%")
    
    print("\n✅ Data quality test PASSED")


def test_anomaly_detection():
    """Test anomaly detection"""
    print("\n" + "="*80)
    print("TEST: Anomaly Detection")
    print("="*80)
    
    monitor = PipelineMonitor()
    anomalies = monitor.detect_anomalies()
    
    print(f"\nAnomalies detected: {anomalies.get('anomalies_detected', 0)}")
    
    for anomaly in anomalies.get('anomalies', []):
        print(f"\n[{anomaly['severity']}] {anomaly['type']}")
        print(f"  Count: {anomaly['count']}")
        print(f"  Message: {anomaly['message']}")
    
    print("\n✅ Anomaly detection test PASSED")


def test_health_report():
    """Test health report generation"""
    print("\n" + "="*80)
    print("TEST: Health Report Generation")
    print("="*80)
    
    monitor = PipelineMonitor()
    report = monitor.generate_health_report()
    
    print(report)
    
    assert len(report) > 0
    assert "FLIGHT PRICE PIPELINE" in report
    print("\n✅ Health report test PASSED")


def test_alert_system():
    """Test alert system"""
    print("\n" + "="*80)
    print("TEST: Alert System")
    print("="*80)
    
    monitor = PipelineMonitor()
    
    # Test different severity levels
    for severity in ['INFO', 'WARNING', 'ERROR']:
        alert = monitor.send_alert(
            alert_type='TEST_ALERT',
            message=f'This is a {severity} level test alert',
            severity=severity
        )
        print(f"\n{severity} alert sent:")
        print(f"  Timestamp: {alert['timestamp']}")
        print(f"  Message: {alert['message']}")
    
    print("\n✅ Alert system test PASSED")


def run_all_tests():
    """Run all monitoring tests"""
    print("\n" + "="*80)
    print("MONITORING SYSTEM TEST SUITE")
    print("="*80)
    
    tests = [
        test_health_check,
        test_performance_metrics,
        test_data_quality,
        test_anomaly_detection,
        test_health_report,
        test_alert_system,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\n❌ Test {test.__name__} FAILED: {str(e)}")
            failed += 1
    
    print("\n" + "="*80)
    print("TEST RESULTS")
    print("="*80)
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")
    
    if failed == 0:
        print("\n✅ ALL TESTS PASSED!")
    else:
        print(f"\n❌ {failed} TEST(S) FAILED")
    
    print("="*80)


if __name__ == '__main__':
    run_all_tests()
