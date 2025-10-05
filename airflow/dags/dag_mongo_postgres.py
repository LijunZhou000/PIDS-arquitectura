from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2

MONGO_URI = "mongodb://mongodb:27017/"
MONGO_DB = "taxis"
MONGO_COLLECTION = "trips"

PG_CONN = {
    "host": "postgres",
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

def create_table_func():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            id SERIAL PRIMARY KEY,
            mongo_id VARCHAR(50) UNIQUE,
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            RatecodeID INT,
            store_and_fwd_flag VARCHAR(1),
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            congestion_surcharge FLOAT,
            total_amount FLOAT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def transfer_data():
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    for doc in collection.find({}):
        cur.execute("""
            INSERT INTO trips (
                mongo_id, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
                PULocationID, DOLocationID, payment_type, fare_amount, extra,
                mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                congestion_surcharge, total_amount
            ) VALUES (
                %(mongo_id)s,
                %(VendorID)s,
                to_timestamp(%(tpep_pickup_datetime)s, 'MM/DD/YYYY HH12:MI:SS AM'),
                to_timestamp(%(tpep_dropoff_datetime)s, 'MM/DD/YYYY HH12:MI:SS AM'),
                %(passenger_count)s, %(trip_distance)s, %(RatecodeID)s, %(store_and_fwd_flag)s,
                %(PULocationID)s, %(DOLocationID)s, %(payment_type)s, %(fare_amount)s, %(extra)s,
                %(mta_tax)s, %(tip_amount)s, %(tolls_amount)s, %(improvement_surcharge)s,
                %(congestion_surcharge)s, %(total_amount)s
            )
            ON CONFLICT (mongo_id) DO NOTHING
        """, {
            "mongo_id": str(doc["_id"]),
            "VendorID": doc.get("VendorID"),
            "tpep_pickup_datetime": doc.get("tpep_pickup_datetime"),   # string "MM/DD/YYYY hh:mm:ss AM/PM"
            "tpep_dropoff_datetime": doc.get("tpep_dropoff_datetime"), # string "MM/DD/YYYY hh:mm:ss AM/PM"
            "passenger_count": doc.get("passenger_count"),
            "trip_distance": doc.get("trip_distance"),
            "RatecodeID": doc.get("RatecodeID"),
            "store_and_fwd_flag": doc.get("store_and_fwd_flag"),
            "PULocationID": doc.get("PULocationID"),
            "DOLocationID": doc.get("DOLocationID"),
            "payment_type": doc.get("payment_type"),
            "fare_amount": doc.get("fare_amount"),
            "extra": doc.get("extra"),
            "mta_tax": doc.get("mta_tax"),
            "tip_amount": doc.get("tip_amount"),
            "tolls_amount": doc.get("tolls_amount"),
            "improvement_surcharge": doc.get("improvement_surcharge"),
            "congestion_surcharge": doc.get("congestion_surcharge"),
            "total_amount": doc.get("total_amount"),
        })

    conn.commit()
    cur.close()
    conn.close()
    mongo_client.close()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="mongo_to_postgres",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 10, 2),
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_func
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=transfer_data
    )

    create_table >> load_data
