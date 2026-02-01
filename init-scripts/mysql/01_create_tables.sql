-- Table: staging_flights
CREATE TABLE staging_flights (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source VARCHAR(100),
    destination VARCHAR(100),
    base_fare DECIMAL(10, 2),
    tax_surcharge DECIMAL(10, 2),
    total_fare DECIMAL(10, 2),
    date_of_journey DATE,
    departure_time TIME,
    arrival_time TIME,
    duration VARCHAR(50),
    stops VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_airline (airline),
    INDEX idx_route (source, destination)
);

-- Table: data_quality_log
CREATE TABLE data_quality_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    check_name VARCHAR(100),
    check_status VARCHAR(20),
    records_checked INT,
    records_failed INT,
    error_details TEXT,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);