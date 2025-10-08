#!/bin/sh

# Contenedor y comando base
SPARK_CONTAINER="spark"
SPARK_SUBMIT="docker exec -it $SPARK_CONTAINER spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,\
org.mongodb:mongodb-driver-sync:5.1.4,\
org.mongodb:mongodb-driver-core:5.1.4,\
org.mongodb:bson:5.1.4 \
  /app/spark_job.py"

# Si no pasas argumento → usa /data/rows.csv como default
if [ $# -eq 0 ]; then
  $SPARK_SUBMIT /data/rows.csv
else
  $SPARK_SUBMIT "$@"
fi