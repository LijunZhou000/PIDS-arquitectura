**Partes 2 y 3 de la práctica 1 de la asignatura de Proyectos en Ingeniería de Datos y Sistemas**

Para realizar el despliegue de la práctica solo es necesario realizar `docker compose up -d --build` desde el directorio raíz de la misma y esperar un par de minutos a que estén todos los contenedores activos y preparados para utilizarse.

Para importar datos en formato csv se usa el comando `sh import.sh [nombre_fichero.csv]`, por defecto importa el archivo /data/rows.

Se puede parar el simulador de viajes con `docker stop simulator`

Las credenciales para acceder a mongo son _admin_ _admin_ y para airflow _airflow_ _airflow_.

En airflow es necesario activar el dag **mongo_to_postgres** que permite pasar los datos de mongo a postgresql, pulsando en el botón de play y a la opción Trigrer DAG.

A continuación se proporcionan los enlaces para acceder a la visualización de los distintos servicios:
- Mongo Express : [localhost:8082](localhost:8082)
- Kafka UI : [locahost:8083](localhost:8083)
- Airflow : [localhost:8087](localhost:8087)
- UI para el chatbot : [localhost:80](localhost:80)
