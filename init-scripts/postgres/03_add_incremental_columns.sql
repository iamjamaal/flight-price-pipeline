-- Migration Script: Add Incremental Loading Support to PostgreSQL Analytics
-- Purpose: Add tracking columns for versioning, history, and UPSERT operations
-- Date: 2026-02-02

-- Add tracking columns to flights_analytics
ALTER TABLE flights_analytics
ADD COLUMN IF NOT EXISTS record_hash VARCHAR(64),
ADD COLUMN IF NOT EXISTS first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS last_updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS version_number INT DEFAULT 1,
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_record_hash ON flights_analytics(record_hash);
CREATE INDEX IF NOT EXISTS idx_last_updated ON flights_analytics(last_updated_date);
CREATE INDEX IF NOT EXISTS idx_is_active ON flights_analytics(is_active);
CREATE INDEX IF NOT EXISTS idx_date_journey ON flights_analytics(date_of_journey);

-- Add unique constraint for UPSERT operations (composite key)
ALTER TABLE flights_analytics
DROP CONSTRAINT IF EXISTS unique_flight_record;

ALTER TABLE flights_analytics
ADD CONSTRAINT unique_flight_record 
UNIQUE (airline, source, destination, date_of_journey, departure_time);

-- Update existing records to have default values
UPDATE flights_analytics 
SET is_active = TRUE,
    version_number = 1,
    first_seen_date = COALESCE(created_at, CURRENT_TIMESTAMP),
    last_updated_date = COALESCE(created_at, CURRENT_TIMESTAMP)
WHERE is_active IS NULL;

-- Create historical tracking table for price changes
CREATE TABLE IF NOT EXISTS flights_analytics_history (
    history_id SERIAL PRIMARY KEY,
    id INT,
    airline VARCHAR(100),
    source VARCHAR(10),
    destination VARCHAR(10),
    base_fare NUMERIC(10,2),
    tax_surcharge NUMERIC(10,2),
    total_fare NUMERIC(10,2),
    date_of_journey DATE,
    departure_time TIME,
    arrival_time TIME,
    duration VARCHAR(20),
    stops VARCHAR(20),
    season VARCHAR(50),
    is_peak_season BOOLEAN,
    record_hash VARCHAR(64),
    version_number INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    change_type VARCHAR(20), -- 'INSERT', 'UPDATE', 'DELETE'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX IF NOT EXISTS idx_history_record_hash ON flights_analytics_history(record_hash);
CREATE INDEX IF NOT EXISTS idx_history_valid_from ON flights_analytics_history(valid_from);


-- Create watermark tracking table
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    last_processed_timestamp TIMESTAMP NOT NULL,
    last_processed_record_count INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initialize watermark for flights_analytics
INSERT INTO pipeline_watermarks (table_name, last_processed_timestamp, last_processed_record_count)
VALUES ('flights_analytics', '1970-01-01 00:00:00', 0)
ON CONFLICT (table_name) DO NOTHING;


-- Add incremental metrics to pipeline_execution_log
ALTER TABLE pipeline_execution_log
ADD COLUMN IF NOT EXISTS records_inserted INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS records_updated INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS records_deleted INT DEFAULT 0,
ADD COLUMN IF NOT EXISTS processing_mode VARCHAR(20) DEFAULT 'FULL_REFRESH';


-- Create view for tracking record changes over time
CREATE OR REPLACE VIEW vw_record_change_history AS
SELECT 
    h.airline,
    h.source,
    h.destination,
    h.date_of_journey,
    h.total_fare,
    h.version_number,
    h.valid_from,
    h.valid_to,
    h.change_type,
    LEAD(h.total_fare) OVER (
        PARTITION BY h.record_hash 
        ORDER BY h.valid_from
    ) as next_fare,
    h.total_fare - LEAD(h.total_fare) OVER (
        PARTITION BY h.record_hash 
        ORDER BY h.valid_from
    ) as fare_change
FROM flights_analytics_history h
ORDER BY h.valid_from DESC;


-- Create view for incremental load statistics
CREATE OR REPLACE VIEW vw_incremental_load_stats AS
SELECT 
    DATE(execution_date) as load_date,
    processing_mode,
    SUM(records_inserted) as total_inserted,
    SUM(records_updated) as total_updated,
    SUM(records_deleted) as total_deleted,
    SUM(records_processed) as total_processed,
    COUNT(*) as number_of_runs
FROM pipeline_execution_log
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(execution_date), processing_mode
ORDER BY load_date DESC;

COMMIT;


-- Verification query
SELECT 
    'Migration completed successfully' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_records,
    COUNT(CASE WHEN record_hash IS NOT NULL THEN 1 END) as records_with_hash,
    MAX(last_updated_date) as latest_update
FROM flights_analytics;
