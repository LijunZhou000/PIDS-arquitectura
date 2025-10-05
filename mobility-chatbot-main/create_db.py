# create_db.py
import sqlite3

conn = sqlite3.connect("mobility.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS vehicles (
    id INTEGER PRIMARY KEY,
    name TEXT,
    status TEXT,
    last_service_date DATE,
    mileage INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS trips (
    id INTEGER PRIMARY KEY,
    vehicle_id INTEGER,
    start_time DATETIME,
    end_time DATETIME,
    distance_km FLOAT
)
""")

cursor.executemany("INSERT INTO vehicles (name, status, last_service_date, mileage) VALUES (?, ?, ?, ?)", [
    ("Van A", "active", "2023-06-15", 123000),
    ("Truck B", "maintenance", "2023-08-20", 85000),
    ("Car C", "active", "2023-05-05", 40000)
])

conn.commit()
conn.close()
