FROM apache/airflow:2.8.1

# Install Docker CLI (required for docker exec commands in DAGs)
USER root
RUN apt-get update && apt-get install -y docker.io && apt-get clean
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/airflow