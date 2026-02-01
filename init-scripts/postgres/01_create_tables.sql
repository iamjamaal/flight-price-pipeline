-- Table: flights_analytics
CREATE TABLE flights_analytics (
    id SERIAL PRIMARY KEY,
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
    season VARCHAR(20),
    is_peak_season BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: kpi_average_fare_by_airline
CREATE TABLE kpi_average_fare_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) UNIQUE,
    avg_base_fare DECIMAL(10, 2),
    avg_total_fare DECIMAL(10, 2),
    booking_count INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: kpi_seasonal_fare_variation
CREATE TABLE kpi_seasonal_fare_variation (
    id SERIAL PRIMARY KEY,
    season VARCHAR(20),
    is_peak_season BOOLEAN,
    avg_fare DECIMAL(10, 2),
    min_fare DECIMAL(10, 2),
    max_fare DECIMAL(10, 2),
    booking_count INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: kpi_popular_routes
CREATE TABLE kpi_popular_routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    destination VARCHAR(100),
    booking_count INT,
    avg_fare DECIMAL(10, 2),
    route_rank INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, destination)
);

-- Table: pipeline_execution_log
CREATE TABLE pipeline_execution_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    status VARCHAR(20),
    records_processed INT,
    execution_time INTERVAL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);