# Use the Bitnami Spark base image
FROM bitnami/spark:3.5.3

# Set working directory
# WORKDIR /opt/spark/app

USER root

# Set environment variables
ENV HOME=/opt/spark/home
ENV IVY_HOME=$HOME/.ivy2

# Create the Ivy cache directory so spark-submit can use it
RUN mkdir -p /opt/spark/home/.ivy2/local

WORKDIR /app
COPY app/ /app

# Añadir conectores
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar /opt/bitnami/spark/jars/