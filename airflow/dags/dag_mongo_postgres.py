from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="spark_mongo_postgres",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark = DockerOperator(
        task_id="spark_job",
        image="spark-job:latest",
        container_name="spark_job",
        auto_remove=True,
        command="spark-submit --master local[*] /app/spark_job.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    run_spark