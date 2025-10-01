from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time

TOPIC = "sim-taxi"

# Esperar a Kafka
consumer = None
for _ in range(10):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="mongo-writer"
        )
        print("✅ Conectado a Kafka")
        break
    except Exception as e:
        print("⏳ Kafka no disponible, reintentando...", e)
        time.sleep(5)

if not consumer:
    raise Exception("❌ No se pudo conectar a Kafka después de varios intentos")

# Esperar a Mongo
mongo_client = None
for _ in range(10):
    try:
        mongo_client = MongoClient("mongodb://mongodb:27017/", serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Conectado a MongoDB")
        break
    except Exception as e:
        print("⏳ Mongo no disponible, reintentando...", e)
        time.sleep(5)

if not mongo_client:
    raise Exception("❌ No se pudo conectar a Mongo después de varios intentos")

db = mongo_client["taxis"]
collection = db["trip"]

print("🚀 Esperando mensajes en Kafka...")

for msg in consumer:
    collection.insert_one(msg.value)
    print("Insertado:", msg.value)
