# Flight Price Pipeline - Step-by-Step Execution Guide

**Complete Setup and Execution Instructions**  
**Project**: Flight Price Data Analytics Pipeline  
**Last Updated**: February 2, 2026

---

## Table of Contents

1. [Prerequisites Check](#1-prerequisites-check)
2. [Project Setup](#2-project-setup)
3. [Docker Environment Setup](#3-docker-environment-setup)
4. [Database Initialization](#4-database-initialization)
5. [Airflow Configuration](#5-airflow-configuration)
6. [Running the Pipeline](#6-running-the-pipeline)
7. [Monitoring and Validation](#7-monitoring-and-validation)
8. [Accessing Data and Analytics](#8-accessing-data-and-analytics)
9. [Troubleshooting](#9-troubleshooting)
10. [Summary Checklist](#10-summary-checklist)

---

## 1. Prerequisites Check

### 1.1 Install Required Software

**Docker Desktop** (Windows/Mac):
```bash
# Download from: https://www.docker.com/products/docker-desktop
# Verify installation
docker --version
docker-compose --version
```

**Expected output**:
```
Docker version 20.10.x or higher
docker-compose version 1.29.x or higher
```

**Python 3.11+** (for local development):
```bash
python --version
pip --version
```

**Git**:
```bash
git --version
```

### 1.2 System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 8 GB | 16 GB |
| Disk Space | 20 GB | 50 GB |
| CPU Cores | 4 | 8 |
| OS | Windows 10/11, macOS, Linux | Any |

### 1.3 Network Ports (Ensure Available)

| Service | Port | Purpose |
|---------|------|---------|
| Airflow Web UI | 8080 | Dashboard |
| MySQL Staging | 3307 | Staging DB |
| PostgreSQL Analytics | 5433 | Analytics DB |

**Check if ports are free (Windows PowerShell)**:
```powershell
netstat -ano | findstr ":8080"
netstat -ano | findstr ":3307"
netstat -ano | findstr ":5433"
# Should return nothing if ports are free
```

**Check if ports are free (Linux/Mac)**:
```bash
lsof -i :8080
lsof -i :3307
lsof -i :5433
# Should return nothing if ports are free
```

---

## 2. Project Setup

### 2.1 Clone Repository

```bash
# Clone from GitHub
git clone https://github.com/iamjamaal/flight-price-pipeline.git
cd flight-price-pipeline
```

### 2.2 Verify Project Structure

**Windows PowerShell**:
```powershell
tree /F
```

**Linux/Mac**:
```bash
ls -R
```

**Expected structure**:
```
flight-price-pipeline/
├── docker/
│   ├── docker-compose.yml        # Container orchestration
│   ├── Dockerfile.airflow         # Airflow image build
│   └── logs/                      # Airflow logs (generated)
├── dags/
│   ├── flight_price_pipeline_dag.py
│   ├── monitoring_dashboard_dag.py
│   └── config/
│       └── pipeline_config.py
├── scripts/
│   ├── data_ingestion.py
│   ├── data_transformation.py
│   ├── data_validation.py
│   ├── kpi_computation.py
│   ├── monitoring.py
│   └── setup_incremental_loading.py
├── data/
│   └── raw/
│       └── Flight_Price_Dataset_of_Bangladesh.csv
├── init-scripts/
│   ├── mysql/
│   │   ├── 01_create_tables.sql
│   │   └── 02_add_incremental_columns.sql
│   └── postgres/
│       ├── 01_create_tables.sql
│       ├── 02_monitoring_views.sql
│       └── 03_add_incremental_columns.sql
└── docs/
    ├── PROJECT_DOCUMENTATION.md
    ├── INCREMENTAL_LOADING_GUIDE.md
    └── EXECUTION_GUIDE.md (this file)
```

### 2.3 Verify Dataset

**Windows PowerShell**:
```powershell
Test-Path "data\raw\Flight_Price_Dataset_of_Bangladesh.csv"
# Expected: True
```

**Linux/Mac**:
```bash
ls -lh data/raw/Flight_Price_Dataset_of_Bangladesh.csv
# Expected: ~6 MB file with 57,000 rows
```

**If missing**: Download the dataset and place it in `data/raw/`

---

## 3. Docker Environment Setup

### 3.1 Navigate to Docker Directory

```bash
cd docker
```

### 3.2 Start All Services

```bash
# Start all containers in detached mode
docker-compose up -d
```

**Expected output**:
```
✔ Network docker_default       Created
✔ Container mysql-staging      Created
✔ Container postgres-analytics Created
✔ Container postgres-airflow   Healthy
✔ Container airflow-init       Created
✔ Container airflow-webserver  Created
✔ Container airflow-scheduler  Created
```

**Startup time**: 30-60 seconds for all health checks to pass

### 3.3 Verify Containers Are Running

```bash
# Check container status
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**Expected output** (all containers should show "Up" or "healthy"):
```
NAMES                STATUS                   PORTS
airflow-webserver    Up (healthy)             0.0.0.0:8080->8080/tcp
airflow-scheduler    Up (healthy)             -
airflow-init         Up                       -
postgres-airflow     Up (healthy)             5432/tcp
postgres-analytics   Up (healthy)             0.0.0.0:5433->5432/tcp
mysql-staging        Up (healthy)             0.0.0.0:3307->3306/tcp
```

### 3.4 Check Container Logs (If Issues)

```bash
# View logs for specific container
docker logs airflow-webserver
docker logs mysql-staging
docker logs postgres-analytics

# Follow logs in real-time
docker logs -f airflow-scheduler
```

**What to look for**:
- ✅ "ready for connections" (MySQL/PostgreSQL)
- ✅ "Listening at: http://0.0.0.0:8080" (Airflow)
- ❌ Any ERROR or FATAL messages

---

## 4. Database Initialization

### 4.1 Verify MySQL Staging Database

**Connect to MySQL container**:
```bash
docker exec -it mysql-staging mysql -u staging_user -pstaging_pass staging_db
```

**Check tables created**:
```sql
SHOW TABLES;
```

**Expected output**:
```
+----------------------+
| Tables_in_staging_db |
+----------------------+
| audit_log            |
| data_quality_log     |
| pipeline_watermarks  |
| staging_flights      |
+----------------------+
```

**Verify staging_flights structure**:
```sql
DESCRIBE staging_flights;
```

**Key columns to verify**:
- `record_hash` (for incremental loading)
- `is_active` (for soft deletes)
- `ingestion_timestamp` (for CDC)
- `source_file` (audit trail)

**Exit MySQL**:
```sql
EXIT;
```

### 4.2 Fix Missing Tables (If Needed)

**If tables don't exist**, run these commands:

**Windows PowerShell**:
```powershell
# Navigate to project root
cd C:\Users\NoahJamalNabila\Desktop\flight-price-pipeline

# Create base tables
Get-Content init-scripts\mysql\01_create_tables.sql | docker exec -i mysql-staging mysql -u staging_user -pstaging_pass staging_db

# Add incremental columns manually
docker exec -it mysql-staging mysql -u staging_user -pstaging_pass staging_db -e "ALTER TABLE staging_flights ADD COLUMN record_hash VARCHAR(64) COMMENT 'MD5 hash for change detection', ADD COLUMN source_file VARCHAR(255) COMMENT 'Source CSV filename', ADD COLUMN ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When record was ingested', ADD COLUMN is_active BOOLEAN DEFAULT TRUE COMMENT 'Soft delete flag'; CREATE INDEX idx_record_hash ON staging_flights(record_hash); CREATE INDEX idx_is_active ON staging_flights(is_active); CREATE INDEX idx_ingestion_timestamp ON staging_flights(ingestion_timestamp);"

# Create watermarks table
docker exec -it mysql-staging mysql -u staging_user -pstaging_pass staging_db -e "CREATE TABLE IF NOT EXISTS pipeline_watermarks (id INT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(100) NOT NULL UNIQUE, last_processed_timestamp TIMESTAMP NOT NULL, last_processed_record_count INT DEFAULT 0, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"
```

**Linux/Mac**:
```bash
# Create base tables
docker exec -i mysql-staging mysql -u staging_user -pstaging_pass staging_db < init-scripts/mysql/01_create_tables.sql

# Add incremental columns
docker exec -i mysql-staging mysql -u staging_user -pstaging_pass staging_db < init-scripts/mysql/02_add_incremental_columns.sql
```

### 4.3 Verify PostgreSQL Analytics Database

**Connect to PostgreSQL container**:
```bash
docker exec -it postgres-analytics psql -U analytics_user -d analytics_db
```

**Check tables**:
```sql
\dt
```

**Expected tables**:
```
 public | flights_analytics              | table
 public | flights_analytics_history      | table
 public | kpi_average_fare_by_airline    | table
 public | kpi_booking_count_by_airline   | table
 public | kpi_popular_routes             | table
 public | kpi_seasonal_fare_variation    | table
 public | pipeline_execution_log         | table
 public | pipeline_watermarks            | table
```

**Check monitoring views**:
```sql
\dv
```

**Expected views** (11 total):
```
 public | vw_data_completeness_check
 public | vw_data_freshness
 public | vw_fare_anomalies
 public | vw_incremental_load_stats      (NEW)
 public | vw_kpi_performance_trends
 public | vw_latest_kpis
 public | vw_pipeline_execution_summary
 public | vw_pipeline_health_overview
 public | vw_processing_efficiency
 public | vw_record_change_history       (NEW)
 public | vw_validation_failure_rate
```

**Exit PostgreSQL**:
```sql
\q
```

---

## 5. Airflow Configuration

### 5.1 Access Airflow Web UI

**Open browser**:
```
http://localhost:8080
```

**Login credentials**:
- Username: `admin`
- Password: `admin`

**First login may take 1-2 minutes** for Airflow to fully initialize.

### 5.2 Verify DAGs Loaded

1. Navigate to **DAGs** page (should load automatically)
2. Look for these DAGs:
   - ✅ `flight_price_pipeline` (main ETL pipeline)
   - ✅ `monitoring_dashboard` (health monitoring)

**If DAGs not showing**:
```bash
# Check if DAG files are detected
docker exec -it airflow-scheduler airflow dags list

# Expected output:
# flight_price_pipeline
# monitoring_dashboard
```

**If still not showing**: Check container logs:
```bash
docker logs airflow-scheduler | grep ERROR
```

### 5.3 Verify DAG Details

**Click on** `flight_price_pipeline` **DAG**:

**Metadata**:
- Schedule: `@daily` (runs at midnight UTC)
- Tags: `flight-price`, `etl`, `analytics`
- Owner: `noah_jamal_nabila`
- Start Date: 2026-01-31

**Tasks** (7 total):
1. `start_pipeline` → Initialize
2. `data_ingestion` → Load CSV to MySQL
3. `data_validation` → Run quality checks
4. `data_transformation` → Transform to PostgreSQL
5. `kpi_computation` → Calculate metrics
6. `log_pipeline_execution` → Log results
7. `end_pipeline` → Cleanup

**Task Dependencies**:
```
start_pipeline → data_ingestion → data_validation → 
data_transformation → kpi_computation → 
log_pipeline_execution → end_pipeline
```

---

## 6. Running the Pipeline

### 6.1 Manual DAG Trigger (First Run)

**Option 1: Via Airflow UI (Recommended)**

1. Go to **DAGs** page
2. Find `flight_price_pipeline`
3. Click **Trigger DAG** button (▶ play icon on the right)
4. Confirm trigger in popup

**Option 2: Via Command Line**
```bash
docker exec -it airflow-scheduler airflow dags trigger flight_price_pipeline
```

### 6.2 Monitor Pipeline Execution

**Watch Real-time Progress**:
1. Click on DAG name: `flight_price_pipeline`
2. View **Graph** tab (visual flow) or **Grid** tab (timeline)
3. Watch task statuses change:
   - ⚪ **Queued** - Waiting to run
   - 🟡 **Running** - Currently executing
   - 🟢 **Success** - Completed successfully
   - 🔴 **Failed** - Error occurred
   - 🟠 **Upstream Failed** - Previous task failed

**Refresh the page** to see status updates (auto-refresh every 30s)

### 6.3 Execution Timeline

**Full Refresh Mode** (Sunday):
```
Time    Task                      Duration    Status
00:00   start_pipeline            1s          ✅
00:01   data_ingestion            2-3 min     🟡 Running...
        - Load 57,000 records
        - TRUNCATE + INSERT
00:04   data_validation           1-2 min     ⚪ Queued
        - Run 6 quality checks
00:06   data_transformation       2-3 min     ⚪ Queued
        - TRUNCATE + Transform all
00:09   kpi_computation           1-2 min     ⚪ Queued
        - Compute 4 KPI tables
00:11   log_pipeline_execution    30s         ⚪ Queued
        - Write logs
00:12   end_pipeline              1s          ⚪ Queued

Total Duration: ~9 minutes
```

**Incremental Mode** (Monday-Saturday):
```
Time    Task                      Duration    Status
00:00   start_pipeline            1s          ✅
00:01   data_ingestion            30s         🟡 Running...
        - Hash check + INSERT
        - Only new/changed records
00:01   data_validation           30s         ⚪ Queued
        - Validate changed records
00:02   data_transformation       30s         ⚪ Queued
        - UPSERT changed records
00:02   kpi_computation           20s         ⚪ Queued
        - Recompute KPIs
00:03   log_pipeline_execution    10s         ⚪ Queued
00:03   end_pipeline              1s          ⚪ Queued

Total Duration: ~2 minutes (77% faster! ⚡)
```

### 6.4 View Task Logs

**For any task**:
1. Click on task box in **Graph** view
2. Click **Log** button in popup
3. View detailed execution logs

**What to check in logs**:

**data_ingestion**:
```
INFO - Starting data ingestion...
INFO - Load mode: INCREMENTAL (Monday-Saturday) or FULL_REFRESH (Sunday)
INFO - Rows inserted: 5700
INFO - Rows unchanged: 51300
INFO - Ingestion completed successfully
```

**data_validation**:
```
INFO - Running 6 validation checks...
INFO - Check 1/6: Negative fare check - PASSED (0 violations)
INFO - Check 2/6: Zero fare check - PASSED (0 violations)
INFO - Validation success rate: 100%
```

**data_transformation**:
```
INFO - Starting transformation...
INFO - Records inserted: 1200
INFO - Records updated: 4500
INFO - Transformation completed successfully
```

**kpi_computation**:
```
INFO - Computing KPIs...
INFO - KPI 1/4: Average fare by airline - COMPUTED
INFO - All 4 KPIs computed successfully
```

### 6.5 Enable Scheduled Runs

**Toggle DAG to Active**:
1. Go to **DAGs** page
2. Find `flight_price_pipeline`
3. Click toggle switch (currently **Paused** → **Active**)
4. Toggle should turn **blue** when active
5. DAG will now run automatically at midnight UTC daily

**Schedule behavior**:
- **Sunday 00:00 UTC**: Full refresh (TRUNCATE + INSERT all 57,000 records)
- **Monday-Saturday 00:00 UTC**: Incremental (INSERT/UPDATE only changed records)

---

## 7. Monitoring and Validation

### 7.1 Check Pipeline Execution Logs

**Via PostgreSQL**:
```bash
docker exec -it postgres-analytics psql -U analytics_user -d analytics_db
```

```sql
-- View latest pipeline runs
SELECT 
    execution_date,
    task_id,
    status,
    records_processed,
    processing_mode,
    execution_time
FROM pipeline_execution_log 
ORDER BY execution_date DESC 
LIMIT 10;
```

**Expected output**:
```
 execution_date      | task_id              | status  | records_processed | processing_mode | execution_time
---------------------+----------------------+---------+-------------------+-----------------+----------------
 2026-02-02 00:00:00 | data_transformation  | SUCCESS | 5700              | INCREMENTAL     | 00:00:28
 2026-02-02 00:00:00 | data_ingestion       | SUCCESS | 5700              | INCREMENTAL     | 00:00:31
```

### 7.2 Compare Incremental vs Full Refresh Performance

```sql
-- Performance comparison
SELECT 
    processing_mode,
    COUNT(*) as total_runs,
    AVG(records_processed) as avg_records,
    AVG(EXTRACT(EPOCH FROM execution_time)) as avg_seconds
FROM pipeline_execution_log
WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY processing_mode;
```

**Expected results**:
```
 processing_mode | total_runs | avg_records | avg_seconds
-----------------+------------+-------------+-------------
 FULL_REFRESH    | 1          | 57000       | 540.0      (9 min)
 INCREMENTAL     | 6          | 5700        | 120.0      (2 min)
```

### 7.3 Validate Data Quality

```sql
-- Check data completeness
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_active THEN 1 END) as active_records,
    MAX(last_updated_date) as latest_update
FROM flights_analytics;
```

**Expected**:
```
 total_records | active_records | latest_update
---------------+----------------+------------------
 57000         | 57000          | 2026-02-02 00:02:15
```

```sql
-- Check incremental loading stats
SELECT * FROM vw_incremental_load_stats
ORDER BY load_date DESC
LIMIT 7;
```

**Expected** (last 7 days):
```
 load_date  | processing_mode | total_inserted | total_updated | total_deleted | number_of_runs
------------+-----------------+----------------+---------------+---------------+----------------
 2026-02-02 | INCREMENTAL     | 1200           | 4500          | 0             | 1
 2026-02-01 | FULL_REFRESH    | 57000          | 0             | 0             | 1
```

### 7.4 View Monitoring Dashboard

**Access monitoring views**:
```sql
-- Pipeline health overview
SELECT * FROM vw_pipeline_health_overview;

-- Data freshness
SELECT * FROM vw_data_freshness;

-- Processing efficiency
SELECT * FROM vw_processing_efficiency;

-- Latest KPIs
SELECT * FROM vw_latest_kpis;
```

### 7.5 Monitor with pgAdmin (Optional)

**Install pgAdmin**: https://www.pgadmin.org/download/

**Connection settings**:
- Host: `localhost`
- Port: `5433`
- Database: `analytics_db`
- Username: `analytics_user`
- Password: `analytics_pass`

**Steps**:
1. Open pgAdmin
2. Right-click **Servers** → **Create** → **Server**
3. **General** tab: Name = `Flight Analytics DB`
4. **Connection** tab: Fill in details above
5. Click **Save**

**Browse**:
- Tables: 8 tables including KPIs
- Views: 11 monitoring views

---

## 8. Accessing Data and Analytics

### 8.1 Query Flight Analytics Data

**Connect to database**:
```bash
docker exec -it postgres-analytics psql -U analytics_user -d analytics_db
```

**Example queries**:

**Top 10 most expensive routes**:
```sql
SELECT 
    airline,
    source,
    destination,
    ROUND(AVG(total_fare), 2) as avg_fare,
    COUNT(*) as flight_count
FROM flights_analytics
WHERE is_active = TRUE
GROUP BY airline, source, destination
ORDER BY avg_fare DESC
LIMIT 10;
```

**Seasonal fare analysis**:
```sql
SELECT 
    season,
    ROUND(avg_fare, 2) as avg_fare,
    ROUND(min_fare, 2) as min_fare,
    ROUND(max_fare, 2) as max_fare,
    flight_count
FROM kpi_seasonal_fare_variation
ORDER BY avg_fare DESC;
```

**Most popular routes**:
```sql
SELECT 
    route,
    total_bookings,
    ROUND(avg_fare, 2) as avg_fare
FROM kpi_popular_routes
ORDER BY total_bookings DESC
LIMIT 10;
```

**Price change history** (incremental loading feature):
```sql
SELECT 
    airline,
    source || ' → ' || destination as route,
    date_of_journey,
    total_fare,
    version_number,
    valid_from,
    change_type
FROM flights_analytics_history
WHERE change_type = 'UPDATE'
ORDER BY valid_from DESC
LIMIT 20;
```

### 8.2 Export Data (CSV)

**Export flights analytics table**:
```bash
# From inside container
docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "\COPY (SELECT * FROM flights_analytics WHERE is_active=TRUE) TO '/tmp/flights_export.csv' CSV HEADER;"

# Copy from container to local machine
docker cp postgres-analytics:/tmp/flights_export.csv ./data/processed/flights_export.csv
```

**Export KPI tables**:
```bash
# Average fare by airline
docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "\COPY kpi_average_fare_by_airline TO '/tmp/kpi_fares.csv' CSV HEADER;"
docker cp postgres-analytics:/tmp/kpi_fares.csv ./data/processed/

# Popular routes
docker exec postgres-analytics psql -U analytics_user -d analytics_db -c "\COPY kpi_popular_routes TO '/tmp/kpi_routes.csv' CSV HEADER;"
docker cp postgres-analytics:/tmp/kpi_routes.csv ./data/processed/
```

### 8.3 Connect BI Tools (Tableau, Power BI, Metabase)

**PostgreSQL Connection Details**:
```
Host: localhost
Port: 5433
Database: analytics_db
Username: analytics_user
Password: analytics_pass
```

**Recommended tables for visualization**:
- `flights_analytics` - Raw flight data with enrichments
- `kpi_average_fare_by_airline` - Airline pricing comparison
- `kpi_seasonal_fare_variation` - Seasonal trends
- `kpi_popular_routes` - Route popularity
- `kpi_booking_count_by_airline` - Booking volumes
- `vw_*` - Pre-built monitoring views

---

## 9. Troubleshooting

### 9.1 Common Issues and Solutions

#### Issue: "Table 'staging_db.staging_flights' doesn't exist"

**Cause**: MySQL init scripts didn't run (volume already exists)

**Solution**: Manually create tables (see Section 4.2 above)

#### Issue: DAGs not showing in Airflow UI

**Cause**: Volume mount paths incorrect or DAG import errors

**Solution**:
```bash
# Check DAG files are accessible
docker exec -it airflow-scheduler ls -la /opt/airflow/dags

# Check for import errors
docker exec -it airflow-scheduler airflow dags list-import-errors

# Restart scheduler
docker restart airflow-scheduler
```

#### Issue: Container won't start

**Cause**: Port already in use

**Solution (Windows)**:
```powershell
# Find process on port 8080
netstat -ano | findstr :8080

# Kill process (replace PID)
taskkill /PID <PID> /F

# Restart containers
cd docker
docker-compose restart
```

#### Issue: Pipeline runs but no data in PostgreSQL

**Cause**: Database connection or transformation error

**Solution**:
```bash
# Check task logs
# In Airflow UI: Click task → Log button

# Verify data in MySQL first
docker exec -it mysql-staging mysql -u staging_user -pstaging_pass -e "SELECT COUNT(*) FROM staging_db.staging_flights;"

# Check transformation logs
docker logs airflow-scheduler | grep transformation
```

### 9.2 Database Connection Issues

**Test MySQL connection**:
```bash
docker exec -it mysql-staging mysql -u staging_user -pstaging_pass -e "SELECT 1;"
```

**Test PostgreSQL connection**:
```bash
docker exec -it postgres-analytics psql -U analytics_user -d analytics_db -c "SELECT 1;"
```

### 9.3 Performance Issues

**Pipeline running slow**:
1. Check system resources: CPU, RAM, Disk
2. Reduce batch size in `pipeline_config.py`
3. Increase Docker memory (Docker Desktop → Settings → Resources)

**Clean up Docker resources**:
```bash
# Remove unused containers and images
docker system prune -a

# Check disk usage
docker system df
```

### 9.4 Reset Everything (Nuclear Option)

**If nothing works, complete reset**:
```bash
cd docker

# Stop and remove everything
docker-compose down -v  # ⚠️ Deletes all data!

# Remove containers
docker rm -f $(docker ps -aq)

# Rebuild and restart
docker-compose build --no-cache
docker-compose up -d

# Wait 60 seconds, then manually create MySQL tables (see Section 4.2)
```

---

## 10. Summary Checklist

### ✅ Initial Setup
- [ ] Docker installed and running
- [ ] Python 3.11+ installed
- [ ] Repository cloned
- [ ] Dataset in `data/raw/` directory
- [ ] Ports 8080, 3307, 5433 available

### ✅ Docker Startup
- [ ] `docker-compose up -d` successful
- [ ] All 6 containers running and healthy
- [ ] No ERROR messages in container logs

### ✅ Database Verification
- [ ] MySQL staging database accessible
- [ ] 4 tables in MySQL (staging_flights, data_quality_log, audit_log, pipeline_watermarks)
- [ ] Incremental columns present (record_hash, is_active, ingestion_timestamp)
- [ ] PostgreSQL analytics database accessible
- [ ] 8 tables + 11 views in PostgreSQL

### ✅ Airflow Configuration
- [ ] Airflow UI accessible at http://localhost:8080
- [ ] Login successful (admin/admin)
- [ ] 2 DAGs visible: flight_price_pipeline, monitoring_dashboard
- [ ] No import errors

### ✅ First Pipeline Run
- [ ] DAG triggered successfully
- [ ] All 7 tasks completed with green status
- [ ] Total execution time ~9 min (full refresh)
- [ ] No task failures

### ✅ Data Validation
- [ ] 57,000 records in `flights_analytics` table
- [ ] All records marked as active
- [ ] 4 KPI tables populated
- [ ] Pipeline execution logged

### ✅ Ongoing Operations
- [ ] DAG enabled for scheduled runs
- [ ] Monitoring dashboard running every 15 min
- [ ] Health reports generated
- [ ] No critical alerts

---

## Quick Reference Commands

### Start/Stop Services
```bash
cd docker
docker-compose up -d          # Start all
docker-compose down           # Stop all
docker-compose restart        # Restart all
docker ps                     # Check status
```

### Database Access
```bash
# MySQL
docker exec -it mysql-staging mysql -u staging_user -pstaging_pass staging_db

# PostgreSQL
docker exec -it postgres-analytics psql -U analytics_user -d analytics_db
```

### Airflow Commands
```bash
# List DAGs
docker exec -it airflow-scheduler airflow dags list

# Trigger DAG
docker exec -it airflow-scheduler airflow dags trigger flight_price_pipeline

# View logs
docker logs -f airflow-scheduler
```

### View Logs
```bash
# Container logs
docker logs <container-name>

# Follow logs
docker logs -f airflow-scheduler

# Task logs (in Airflow UI)
# Click task → Log button
```

---

## Next Steps

After successful setup:

1. **Enable Scheduled Runs**: Toggle DAG to active in Airflow UI
2. **Monitor Performance**: Review execution metrics daily
3. **Analyze Price Trends**: Use `vw_record_change_history` for insights
4. **Connect BI Tools**: Link Tableau/Power BI for visualizations
5. **Set Up Alerts**: Configure email/Slack notifications

---

**Success!** 🎉 Your Flight Price Pipeline is now operational with incremental loading, monitoring, and analytics capabilities!

**Need Help?** Check the troubleshooting section or review container logs for detailed error messages.

**Documentation**: 
- [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) - Complete technical details
- [INCREMENTAL_LOADING_GUIDE.md](INCREMENTAL_LOADING_GUIDE.md) - CDC implementation guide
