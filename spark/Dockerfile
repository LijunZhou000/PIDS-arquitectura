FROM bitnami/spark:latest

# Copiar app al contenedor
WORKDIR /app
COPY app/ /app

# Añadir conectores
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar /opt/bitnami/spark/jars/