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