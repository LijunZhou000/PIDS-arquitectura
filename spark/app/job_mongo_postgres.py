from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("MongoToPostgres") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,"
                "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # Leer de Mongo
    df = spark.read \
        .format("mongodb") \
        .option("uri", "mongodb://mongo:27017") \
        .option("database", "testdb") \
        .option("collection", "users") \
        .load()

    df.show()

    # Escribir en Postgres
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/testdb") \
        .option("dbtable", "users_copy") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("overwrite") \
        .save()

    spark.stop()

if __name__ == "__main__":
    main()
