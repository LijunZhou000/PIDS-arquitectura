FROM apache/airflow:2.4.1

USER root

# Install Java (for Spark) and procps (for 'ps' command)
RUN rm -f /etc/apt/sources.list.d/mysql.list \
    && apt-get update \
    && apt-get install -y openjdk-11-jdk-headless procps \
    && apt-get clean


# Set JAVA_HOME explicitly
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Optional: install other dependencies (already in _PIP_ADDITIONAL_REQUIREMENTS)
# RUN pip install mlflow==1.29.0 iso8601 pyspark findspark

USER airflow