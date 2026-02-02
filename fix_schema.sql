-- Add missing columns to kpi_seasonal_fare_variation
ALTER TABLE kpi_seasonal_fare_variation 
ADD COLUMN IF NOT EXISTS median_fare DECIMAL(10, 2), 
ADD COLUMN IF NOT EXISTS std_dev_fare DECIMAL(10, 2);

-- Add missing columns to kpi_popular_routes
ALTER TABLE kpi_popular_routes 
ADD COLUMN IF NOT EXISTS min_fare DECIMAL(10, 2), 
ADD COLUMN IF NOT EXISTS max_fare DECIMAL(10, 2), 
ADD COLUMN IF NOT EXISTS route VARCHAR(250);

-- Create kpi_booking_count_by_airline if not exists
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) UNIQUE,
    total_bookings INT,
    peak_season_bookings INT,
    off_season_bookings INT,
    market_share_percentage DECIMAL(5, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
