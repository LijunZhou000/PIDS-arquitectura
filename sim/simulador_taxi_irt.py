import json
import random
import time
import uuid
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer

# -----------------------
# Configuración de logging
# -----------------------
logging.basicConfig(
    filename="taxi_trips.log",       # fichero de logs
    level=logging.INFO,              # nivel de log
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# Configuración del productor Kafka
# -----------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "taxi_trips"

def generar_inicio(trip_id):
    """Genera un mensaje de inicio de trayecto"""
    now = datetime.now()
    return {
        "event_type": "start",
        "trip_id": trip_id,
        "VendorID": random.choice([1, 2]),
        "tpep_pickup_datetime": now.isoformat(),
        "passenger_count": random.randint(1, 4),
        "PULocationID": random.randint(1, 265)
    }

def generar_fin(trip_id, pickup_time):
    """Genera un mensaje de fin de trayecto"""
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 45))
    return {
        "event_type": "end",
        "trip_id": trip_id,
        "tpep_dropoff_datetime": dropoff_time.isoformat(),
        "trip_distance": round(random.uniform(1.0, 15.0), 2),
        "RatecodeID": random.choice([1, 2, 3, 4, 5]),
        "store_and_fwd_flag": random.randint(0, 1),
        "DOLocationID": random.randint(1, 265),
        "payment_type": random.choice([1, 2]),
        "fare_amount": round(random.uniform(5, 50), 2),
        "extra": round(random.uniform(0, 5), 2),
        "mta_tax": 0.5,
        "tip_amount": round(random.uniform(0, 10), 2),
        "tolls_amount": round(random.uniform(0, 5), 2),
        "improvement_surcharge": 0.3,
        "total_amount": round(random.uniform(10, 70), 2),
        "congestion_surcharge": round(random.uniform(0, 3), 2)
    }

def enviar_viaje():
    """Simula un viaje completo con inicio y fin"""
    trip_id = str(uuid.uuid4())
    inicio = generar_inicio(trip_id)
    
    # Enviar inicio
    producer.send(TOPIC_NAME, value=inicio)
    logging.info(f"INICIO viaje trip_id={trip_id} datos={inicio}")
    print(f"[Kafka] Enviado inicio de viaje {trip_id}")

    # Espera simulada
    time.sleep(3)

    # Generar fin
    pickup_time = datetime.fromisoformat(inicio["tpep_pickup_datetime"])
    fin = generar_fin(trip_id, pickup_time)
    
    # Enviar fin
    producer.send(TOPIC_NAME, value=fin)
    logging.info(f"FIN viaje trip_id={trip_id} datos={fin}")
    print(f"[Kafka] Enviado fin de viaje {trip_id}")

if __name__ == "__main__":
    for _ in range(5):  # simular 5 viajes
        enviar_viaje()
        time.sleep(2)

    producer.flush()