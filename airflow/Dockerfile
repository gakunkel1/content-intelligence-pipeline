FROM apache/airflow:3.2.0-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /home/airflow/requirements.txt
RUN pip install -r /home/airflow/requirements.txt