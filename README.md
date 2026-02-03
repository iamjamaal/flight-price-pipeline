# Flight Price Analysis Pipeline

End-to-end data pipeline for analyzing flight price data from Bangladesh using Apache Airflow.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Components](#pipeline-components)
- [KPIs](#kpis)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline that:
1. Ingests flight price data from CSV files
2. Validates data quality
3. Transforms and enriches data
4. Computes key performance indicators
5. Stores results in a PostgreSQL analytics database

## Architecture
CSV Data → MySQL (Staging) → Validation → Transformation → PostgreSQL (Analytics) → KPIs
↓
Airflow Orchestration

### Technology Stack
- **Orchestration**: Apache Airflow 2.8.0
- **Staging Database**: MySQL 8.0
- **Analytics Database**: PostgreSQL 15
- **Processing**: Python 3.11, Pandas
- **Deployment**: Docker Compose

## Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- 8GB RAM minimum
- 20GB free disk space
- Dataset: Flight Price Dataset of Bangladesh (Kaggle)

## Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd flight-price-pipeline
```

### 2. Download Dataset

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)

Place the CSV file at:
data/raw/Flight_Price_Dataset_of_Bangladesh.csv

### 3. Configure Environment

Copy `.env.example` to `.env` and adjust settings if needed:
```bash
cp .env.example .env
```

### 4. Start Services
```bash
# Initialize Airflow (first time only)
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 5. Access Airflow UI

Navigate to: http://localhost:8080

- Username: `admin`
- Password: `admin`

## Usage

### Running the Pipeline

1. **Via Airflow UI**:
   - Go to http://localhost:8080
   - Find the DAG: `flight_price_pipeline`
   - Toggle it ON
   - Click "Trigger DAG"

2. **Via CLI**:
```bash
   docker-compose exec airflow-scheduler airflow dags trigger flight_price_pipeline
```

### Monitoring

- **Airflow UI**: Monitor task execution and logs
- **Database**: Query analytics tables directly
```bash
# Connect to PostgreSQL
docker-compose exec postgres-analytics psql -U analytics_user -d analytics_db

# Sample query
SELECT * FROM kpi_average_fare_by_airline ORDER BY avg_total_fare DESC;
```

## Pipeline Components

### 1. Data Ingestion
- **Input**: CSV file
- **Output**: MySQL `staging_flights` table
- **Duration**: ~2-5 minutes
- **Key Features**:
  - Batch processing
  - Column standardization
  - Error logging

### 2. Data Validation
- **Checks**:
  - Required columns exist
  - No null values in critical fields
  - Data type validation
  - Fare consistency
  - City name validation
  - Duplicate detection
- **Output**: `data_quality_log` table

### 3. Data Transformation
- **Transformations**:
  - Total fare calculation/verification
  - Seasonal classification
  - Peak season identification
  - Data cleaning
- **Output**: PostgreSQL `flights_analytics` table

### 4. KPI Computation
- **Metrics**: See [KPIs](#kpis) section
- **Output**: Dedicated KPI tables

## KPIs

### 1. Average Fare by Airline
- Average base fare, tax, and total fare per airline
- Min/max fare ranges
- Booking count

**Table**: `kpi_average_fare_by_airline`

### 2. Seasonal Fare Variation
- Fare statistics by season
- Peak vs off-peak comparison
- Standard deviation analysis

**Table**: `kpi_seasonal_fare_variation`

### 3. Most Popular Routes
- Top 20 routes by booking count
- Average fare per route
- Route ranking

**Table**: `kpi_popular_routes`

### 4. Booking Count by Airline
- Total bookings per airline
- Peak/off-season breakdown
- Market share percentage

**Table**: `kpi_booking_count_by_airline`

## Monitoring

### Logs
```bash
# Airflow scheduler logs
docker-compose logs -f airflow-scheduler

# Airflow webserver logs
docker-compose logs -f airflow-webserver

# Database logs
docker-compose logs -f postgres-analytics
docker-compose logs -f mysql-staging
```

### Health Checks
```bash
# Check service health
docker-compose ps

# Check database connectivity
docker-compose exec postgres-analytics pg_isready
docker-compose exec mysql-staging mysqladmin ping
```

## Troubleshooting

### Common Issues

#### 1. Services won't start
```bash
# Check Docker resources
docker system df

# Restart all services
docker-compose down
docker-compose up -d
```

#### 2. Database connection errors
```bash
# Verify database is running
docker-compose ps postgres-analytics mysql-staging

# Check logs
docker-compose logs postgres-analytics
docker-compose logs mysql-staging
```

#### 3. DAG not appearing
```bash
# Check for Python syntax errors
docker-compose exec airflow-scheduler airflow dags list

# Restart scheduler
docker-compose restart airflow-scheduler
```

#### 4. Task failures
- Check task logs in Airflow UI
- Verify data file exists in correct location
- Check database connectivity
- Review validation errors in `data_quality_log` table

