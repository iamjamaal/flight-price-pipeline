-- Migration Script: Add Incremental Loading Support to MySQL Staging
-- Purpose: Add tracking columns for change detection and incremental processing


USE staging_db;

-- Add tracking columns to staging_flights
ALTER TABLE staging_flights
ADD COLUMN IF NOT EXISTS record_hash VARCHAR(64) COMMENT 'MD5 hash for change detection',
ADD COLUMN IF NOT EXISTS source_file VARCHAR(255) COMMENT 'Source CSV filename',
ADD COLUMN IF NOT EXISTS ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When record was ingested',
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE COMMENT 'Soft delete flag',
ADD INDEX IF NOT EXISTS idx_record_hash (record_hash),
ADD INDEX IF NOT EXISTS idx_is_active (is_active),
ADD INDEX IF NOT EXISTS idx_ingestion_timestamp (ingestion_timestamp);

-- Update existing records to have is_active = TRUE
UPDATE staging_flights SET is_active = TRUE WHERE is_active IS NULL;

-- Create table for tracking pipeline watermarks
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    last_processed_timestamp TIMESTAMP NOT NULL,
    last_processed_record_count INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_table (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Initialize watermark for staging_flights
INSERT INTO pipeline_watermarks (table_name, last_processed_timestamp, last_processed_record_count)
VALUES ('staging_flights', '1970-01-01 00:00:00', 0)
ON DUPLICATE KEY UPDATE last_processed_timestamp = last_processed_timestamp;

-- Add incremental loading statistics to audit_log
ALTER TABLE audit_log
ADD COLUMN IF NOT EXISTS records_inserted INT DEFAULT 0 COMMENT 'New records inserted',
ADD COLUMN IF NOT EXISTS records_updated INT DEFAULT 0 COMMENT 'Existing records updated',
ADD COLUMN IF NOT EXISTS records_deleted INT DEFAULT 0 COMMENT 'Records soft-deleted';

COMMIT;

-- Verification query
SELECT 
    'Migration completed successfully' as status,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_records,
    SUM(CASE WHEN record_hash IS NOT NULL THEN 1 ELSE 0 END) as records_with_hash
FROM staging_flights;
