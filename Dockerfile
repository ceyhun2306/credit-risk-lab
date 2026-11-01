
FROM apache/airflow:3.1.0-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git bash curl unzip openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt
