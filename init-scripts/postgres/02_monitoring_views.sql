-- Monitoring & Observability Views
-- Create materialized views for monitoring dashboards

-- View 1: Pipeline Execution Summary
CREATE OR REPLACE VIEW vw_pipeline_execution_summary AS
SELECT 
    DATE(execution_date) as execution_date,
    COUNT(DISTINCT execution_date) as total_runs,
    COUNT(DISTINCT CASE WHEN status IN ('SUCCESS', 'PASSED') THEN execution_date END) as successful_runs,
    COUNT(DISTINCT CASE WHEN status = 'FAILED' THEN execution_date END) as failed_runs,
    SUM(records_processed) as total_records_processed,
    AVG(EXTRACT(EPOCH FROM execution_time)) as avg_execution_time,
    MIN(EXTRACT(EPOCH FROM execution_time)) as min_execution_time,
    MAX(EXTRACT(EPOCH FROM execution_time)) as max_execution_time
FROM pipeline_execution_log
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(execution_date)
ORDER BY execution_date DESC;

COMMENT ON VIEW vw_pipeline_execution_summary IS 'Daily pipeline execution summary for the last 30 days';


-- View 2: Task Performance Metrics
CREATE OR REPLACE VIEW vw_task_performance AS
SELECT 
    task_id,
    COUNT(*) as execution_count,
    SUM(CASE WHEN status IN ('SUCCESS', 'PASSED') THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failure_count,
    ROUND(SUM(CASE WHEN status IN ('SUCCESS', 'PASSED') THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) as success_rate,
    ROUND(AVG(EXTRACT(EPOCH FROM execution_time))::NUMERIC, 2) as avg_execution_time,
    ROUND(MIN(EXTRACT(EPOCH FROM execution_time))::NUMERIC, 2) as min_execution_time,
    ROUND(MAX(EXTRACT(EPOCH FROM execution_time))::NUMERIC, 2) as max_execution_time,
    ROUND(STDDEV(EXTRACT(EPOCH FROM execution_time))::NUMERIC, 2) as stddev_execution_time,
    MAX(execution_date) as last_execution
FROM pipeline_execution_log
WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY task_id
ORDER BY task_id;

COMMENT ON VIEW vw_task_performance IS 'Performance metrics by task for the last 7 days';


-- View 3: Data Quality Trends
CREATE OR REPLACE VIEW vw_data_quality_overview AS
SELECT 
    season,
    is_peak_season,
    COUNT(*) as record_count,
    ROUND(AVG(total_fare)::NUMERIC, 2) as avg_fare,
    ROUND(MIN(total_fare)::NUMERIC, 2) as min_fare,
    ROUND(MAX(total_fare)::NUMERIC, 2) as max_fare,
    ROUND(STDDEV(total_fare)::NUMERIC, 2) as stddev_fare,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare)::NUMERIC, 2) as median_fare,
    COUNT(DISTINCT airline) as unique_airlines,
    COUNT(DISTINCT source) as unique_sources,
    COUNT(DISTINCT destination) as unique_destinations,
    COUNT(CASE WHEN date_of_journey IS NULL THEN 1 END) as missing_dates
FROM flights_analytics
GROUP BY season, is_peak_season
ORDER BY season, is_peak_season;

COMMENT ON VIEW vw_data_quality_overview IS 'Data quality and distribution metrics by season';


-- View 4: Top Routes Performance
CREATE OR REPLACE VIEW vw_top_routes_performance AS
SELECT 
    source || ' -> ' || destination as route,
    COUNT(*) as booking_count,
    ROUND(AVG(total_fare), 2) as avg_fare,
    ROUND(MIN(total_fare), 2) as min_fare,
    ROUND(MAX(total_fare), 2) as max_fare,
    COUNT(DISTINCT airline) as airlines_serving,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM flights_analytics) * 100, 2) as market_share_pct
FROM flights_analytics
GROUP BY source, destination
HAVING COUNT(*) >= 100  -- Only routes with significant volume
ORDER BY booking_count DESC
LIMIT 20;

COMMENT ON VIEW vw_top_routes_performance IS 'Top 20 routes with performance metrics';


-- View 5: Airline Performance Comparison
CREATE OR REPLACE VIEW vw_airline_performance AS
SELECT 
    airline,
    COUNT(*) as total_bookings,
    ROUND(AVG(total_fare), 2) as avg_fare,
    ROUND(MIN(total_fare), 2) as min_fare,
    ROUND(MAX(total_fare), 2) as max_fare,
    ROUND(AVG(CASE WHEN is_peak_season THEN total_fare END), 2) as avg_peak_fare,
    ROUND(AVG(CASE WHEN NOT is_peak_season THEN total_fare END), 2) as avg_offpeak_fare,
    COUNT(DISTINCT source || '-' || destination) as routes_served,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM flights_analytics) * 100, 2) as market_share
FROM flights_analytics
GROUP BY airline
ORDER BY total_bookings DESC;

COMMENT ON VIEW vw_airline_performance IS 'Comprehensive airline performance metrics';


-- View 6: Recent Pipeline Activity
CREATE OR REPLACE VIEW vw_recent_pipeline_activity AS
SELECT 
    execution_date,
    task_id,
    status,
    records_processed,
    execution_time,
    CASE 
        WHEN EXTRACT(EPOCH FROM execution_time) < 60 THEN EXTRACT(EPOCH FROM execution_time) || 's'
        WHEN EXTRACT(EPOCH FROM execution_time) < 3600 THEN ROUND(EXTRACT(EPOCH FROM execution_time) / 60, 1) || 'm'
        ELSE ROUND(EXTRACT(EPOCH FROM execution_time) / 3600, 1) || 'h'
    END as execution_time_formatted,
    error_message
FROM pipeline_execution_log
WHERE execution_date >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY execution_date DESC;

COMMENT ON VIEW vw_recent_pipeline_activity IS 'Pipeline activity in the last 24 hours';



-- View 7: Seasonal Booking Patterns
CREATE OR REPLACE VIEW vw_seasonal_patterns AS
SELECT 
    EXTRACT(MONTH FROM date_of_journey) as month_num,
    TO_CHAR(date_of_journey, 'Month') as month_name,
    season,
    COUNT(*) as bookings,
    ROUND(AVG(total_fare), 2) as avg_fare,
    ROUND(MIN(total_fare), 2) as min_fare,
    ROUND(MAX(total_fare), 2) as max_fare,
    COUNT(DISTINCT airline) as airlines_operating
FROM flights_analytics
WHERE date_of_journey IS NOT NULL
GROUP BY EXTRACT(MONTH FROM date_of_journey), TO_CHAR(date_of_journey, 'Month'), season
ORDER BY month_num;

COMMENT ON VIEW vw_seasonal_patterns IS 'Monthly booking patterns and seasonal analysis';


-- View 8: Data Completeness Report
CREATE OR REPLACE VIEW vw_data_completeness AS
SELECT 
    'Total Records' as metric,
    COUNT(*)::TEXT as value,
    '100%' as percentage
FROM flights_analytics
UNION ALL
SELECT 
    'Records with Dates',
    COUNT(date_of_journey)::TEXT,
    ROUND(COUNT(date_of_journey)::NUMERIC / COUNT(*) * 100, 2) || '%'
FROM flights_analytics
UNION ALL
SELECT 
    'Records with Airline',
    COUNT(airline)::TEXT,
    ROUND(COUNT(airline)::NUMERIC / COUNT(*) * 100, 2) || '%'
FROM flights_analytics
UNION ALL
SELECT 
    'Records with Fare',
    COUNT(total_fare)::TEXT,
    ROUND(COUNT(total_fare)::NUMERIC / COUNT(*) * 100, 2) || '%'
FROM flights_analytics
UNION ALL
SELECT 
    'Records with Season',
    COUNT(CASE WHEN season != 'Unknown' AND season IS NOT NULL THEN 1 END)::TEXT,
    ROUND(COUNT(CASE WHEN season != 'Unknown' AND season IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) || '%'
FROM flights_analytics;

COMMENT ON VIEW vw_data_completeness IS 'Data completeness metrics for key fields';


-- View 9: Anomaly Detection
CREATE OR REPLACE VIEW vw_potential_anomalies AS
WITH fare_stats AS (
    SELECT 
        AVG(total_fare) as mean_fare,
        STDDEV(total_fare) as stddev_fare
    FROM flights_analytics
)
SELECT 
    'Fare Outliers' as anomaly_type,
    COUNT(*) as count,
    'Fares > 3 std deviations from mean' as description
FROM flights_analytics, fare_stats
WHERE total_fare > (mean_fare + 3 * stddev_fare)
   OR total_fare < (mean_fare - 3 * stddev_fare)
UNION ALL
SELECT 
    'Missing Dates',
    COUNT(*),
    'Records without date_of_journey'
FROM flights_analytics
WHERE date_of_journey IS NULL
UNION ALL
SELECT 
    'Unknown Season',
    COUNT(*),
    'Records with season = Unknown or NULL'
FROM flights_analytics
WHERE season = 'Unknown' OR season IS NULL
UNION ALL
SELECT 
    'Zero/Negative Fares',
    COUNT(*),
    'Records with total_fare <= 0'
FROM flights_analytics
WHERE total_fare <= 0;

COMMENT ON VIEW vw_potential_anomalies IS 'Detection of potential data anomalies';


-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_pipeline_log_execution_date 
ON pipeline_execution_log(execution_date DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_task_status 
ON pipeline_execution_log(task_id, status);

CREATE INDEX IF NOT EXISTS idx_flights_season 
ON flights_analytics(season, is_peak_season);

CREATE INDEX IF NOT EXISTS idx_flights_date 
ON flights_analytics(date_of_journey);

-- Analysis complete notification
DO $$
BEGIN
    RAISE NOTICE 'Monitoring views and indexes created successfully';
END $$;
