import time, json, random
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaError

BROKER = "kafka:9092"
TOPIC = "sim-taxi"

def wait_for_kafka(broker, topic, timeout=60):
    """Espera a que Kafka esté listo y el topic exista"""
    start_time = time.time()
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker)
            topics = admin.list_topics()
            if topic in topics:
                print(f"✅ Kafka está listo y el topic '{topic}' existe.")
                break
            else:
                print(f"⚠️ Kafka listo, pero el topic '{topic}' aún no existe. Reintentando...")
        except NoBrokersAvailable:
            print("⏳ Esperando a que Kafka esté disponible...")
        except KafkaError as e:
            print(f"⚠️ Error temporal con Kafka: {e}")
        
        if time.time() - start_time > timeout:
            raise TimeoutError(f"❌ Timeout esperando a Kafka y al topic '{topic}'")
        
        time.sleep(3)


def generar_trip():
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=random.randint(1, 30))

    trip = {
        "VendorID": random.choice([1, 2]),
        "tpep_pickup_datetime": start_time.strftime("%m/%d/%Y %I:%M:%S %p"),
        "tpep_dropoff_datetime": end_time.strftime("%m/%d/%Y %I:%M:%S %p"),
        "passenger_count": random.randint(1, 4),
        "trip_distance": round(random.uniform(0.5, 15.0), 2),
        "RatecodeID": random.choice([1, 2, 3, 4, 5, 6]),
        "store_and_fwd_flag": random.choice(["Y", "N"]),
        "PULocationID": random.randint(1, 250),
        "DOLocationID": random.randint(1, 250),
        "payment_type": random.choice([1, 2]),
        "fare_amount": round(random.uniform(5, 50), 2),
        "extra": round(random.uniform(0, 5), 2),
        "mta_tax": 0.5,
        "tip_amount": round(random.uniform(0, 15), 2),
        "tolls_amount": round(random.uniform(0, 10), 2),
        "improvement_surcharge": 0.3,
        "congestion_surcharge": random.choice([0, 2.5])
    }
    trip["total_amount"] = (
        trip["fare_amount"] + trip["extra"] + trip["mta_tax"] +
        trip["tip_amount"] + trip["tolls_amount"] +
        trip["improvement_surcharge"] + trip["congestion_surcharge"]
    )
    return trip


if __name__ == "__main__":
    # 🔹 Primero esperamos a Kafka
    wait_for_kafka(BROKER, TOPIC)

    # 🔹 Luego creamos el Producer
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # 🔹 Loop de simulación
    while True:
        trip = generar_trip()
        producer.send(TOPIC, trip)
        print("Enviado:", trip)
        time.sleep(1)
