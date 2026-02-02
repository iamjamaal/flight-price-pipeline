# Monitoring & Observability Setup - PowerShell
# Run this script to set up monitoring

Write-Host "=========================================="
Write-Host "Setting up Monitoring & Observability"
Write-Host "=========================================="



# Step 1: Initialize monitoring views in PostgreSQL
Write-Host ""
Write-Host "Step 1: Creating monitoring views in PostgreSQL..." -ForegroundColor Cyan
docker-compose exec -T postgres-analytics psql -U analytics_user -d analytics_db -f /opt/airflow/init-scripts/postgres/02_monitoring_views.sql

if ($LASTEXITCODE -eq 0) {
    Write-Host "Monitoring views created successfully" -ForegroundColor Green
} else {
    Write-Host "Failed to create monitoring views" -ForegroundColor Red
    exit 1
}

# Step 2: Test monitoring script
Write-Host ""
Write-Host "Step 2: Testing monitoring script..." -ForegroundColor Cyan
docker-compose exec -T airflow-webserver python /opt/airflow/scripts/monitoring.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "Monitoring script test passed" -ForegroundColor Green
} else {
    Write-Host "Failed to run monitoring script test" -ForegroundColor Red
    exit 1
}

# Step 3: Run monitoring test suite
Write-Host ""
Write-Host "Step 3: Running monitoring test suite..." -ForegroundColor Cyan
docker-compose exec -T airflow-webserver python /opt/airflow/tests/test_monitoring.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "Monitoring tests passed" -ForegroundColor Green
} else {
    Write-Host "Some monitoring tests may have failed (check logs)" -ForegroundColor Yellow
}

# Step 4: Enable monitoring DAG
Write-Host ""
Write-Host "Step 4: Enabling monitoring DAG..." -ForegroundColor Cyan
Write-Host "Please enable the 'monitoring_dashboard' DAG in Airflow UI:" -ForegroundColor Yellow
Write-Host "  URL: http://localhost:8080" -ForegroundColor White
Write-Host "  DAG: monitoring_dashboard" -ForegroundColor White
Write-Host "  Schedule: Every 15 minutes" -ForegroundColor White

# Step 5: Show available monitoring queries
Write-Host ""
Write-Host "=========================================="
Write-Host "Monitoring Setup Complete!" -ForegroundColor Green
Write-Host "=========================================="
Write-Host ""
Write-Host "Available monitoring views:" -ForegroundColor Cyan
Write-Host "  • vw_pipeline_execution_summary"
Write-Host "  • vw_task_performance"
Write-Host "  • vw_data_quality_overview"
Write-Host "  • vw_top_routes_performance"
Write-Host "  • vw_airline_performance"
Write-Host "  • vw_recent_pipeline_activity"
Write-Host "  • vw_seasonal_patterns"
Write-Host "  • vw_data_completeness"
Write-Host "  • vw_potential_anomalies"
Write-Host ""
Write-Host "Quick commands:" -ForegroundColor Cyan
Write-Host "  Health check:  docker-compose exec airflow-webserver python /opt/airflow/scripts/monitoring.py"
Write-Host "  Run tests:     docker-compose exec airflow-webserver python /opt/airflow/tests/test_monitoring.py"
Write-Host "  View metrics:  docker-compose exec postgres-analytics psql -U analytics_user -d analytics_db"
Write-Host ""
Write-Host "Documentation: docs\MONITORING_GUIDE.md" -ForegroundColor Yellow
Write-Host "=========================================="
