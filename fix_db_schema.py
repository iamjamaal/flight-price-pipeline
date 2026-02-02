"""Fix database schema by adding missing columns"""
import psycopg2

# Connect to database
conn = psycopg2.connect(
    host='localhost',
    port=5433,
    database='analytics_db',
    user='analytics_user',
    password='analytics_pass'
)

cursor = conn.cursor()

# Fix kpi_seasonal_fare_variation
print("Fixing kpi_seasonal_fare_variation...")
cursor.execute("""
    ALTER TABLE kpi_seasonal_fare_variation 
    ADD COLUMN IF NOT EXISTS median_fare DECIMAL(10, 2), 
    ADD COLUMN IF NOT EXISTS std_dev_fare DECIMAL(10, 2);
""")
conn.commit()
print("✓ Added median_fare and std_dev_fare columns")

# Fix kpi_popular_routes
print("\nFixing kpi_popular_routes...")
cursor.execute("""
    ALTER TABLE kpi_popular_routes 
    ADD COLUMN IF NOT EXISTS min_fare DECIMAL(10, 2), 
    ADD COLUMN IF NOT EXISTS max_fare DECIMAL(10, 2), 
    ADD COLUMN IF NOT EXISTS route VARCHAR(250);
""")
conn.commit()
print("✓ Added min_fare, max_fare, and route columns")

# Create kpi_booking_count_by_airline
print("\nCreating kpi_booking_count_by_airline...")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
        id SERIAL PRIMARY KEY,
        airline VARCHAR(100) UNIQUE,
        total_bookings INT,
        peak_season_bookings INT,
        off_season_bookings INT,
        market_share_percentage DECIMAL(5, 2),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
conn.commit()
print("✓ Created kpi_booking_count_by_airline table")

# Verify changes
print("\n=== Verification ===")
cursor.execute("""
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE table_name IN ('kpi_seasonal_fare_variation', 'kpi_popular_routes', 'kpi_booking_count_by_airline')
    ORDER BY table_name, ordinal_position;
""")
results = cursor.fetchall()
current_table = None
for table, column in results:
    if table != current_table:
        print(f"\n{table}:")
        current_table = table
    print(f"  - {column}")

cursor.close()
conn.close()
print("\n✓ All schema fixes applied successfully!")
