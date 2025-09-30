import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    csv_file = "/data/rows.csv"
else:
    csv_file = sys.argv[1]


spark = (
    SparkSession.builder
    .appName("CSVtoMongo")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/taxis.trips")
    .getOrCreate()
)

# Leer CSV pasado por parámetro
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Guardar en MongoDB
df.write.format("mongodb").mode("append").save()

print(f"Datos de {csv_file} insertados en MongoDB")
spark.stop()
