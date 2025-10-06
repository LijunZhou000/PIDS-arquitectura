# Usa la imagen oficial de Bitnami Spark
FROM bitnami/spark:3.5.3

# Ejecuta como root para poder copiar y añadir archivos
USER root

# Variables de entorno para el Ivy cache (si usas spark-submit con --packages)
ENV HOME=/opt/spark/home
ENV IVY_HOME=$HOME/.ivy2

# Crea el directorio de cache de Ivy (para dependencias descargadas por Spark)
RUN mkdir -p /opt/spark/home/.ivy2/local

# Copia tu aplicación Spark dentro del contenedor
WORKDIR /app
COPY app/ /app

# 🔌 Añadir conectores necesarios a Spark
# PostgreSQL JDBC driver → permite lecturas/escrituras desde/hacia Postgres
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
    /opt/bitnami/spark/jars/

# MongoDB Spark connector + dependencias Java 5.1.4 (últimos estables)
ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.1/mongo-spark-connector_2.12-10.4.1.jar \
    /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar \
    /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar \
    /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/bson/5.1.4/bson-5.1.4.jar \
    /opt/bitnami/spark/jars/

# (Opcional) Evita advertencias de permisos y mejora compatibilidad
RUN chmod -R 755 /opt/bitnami/spark/jars && \
    mkdir -p /tmp/spark-events

# Establece la variable de entorno del classpath de Spark
ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"

# Directorio por defecto
WORKDIR /app
