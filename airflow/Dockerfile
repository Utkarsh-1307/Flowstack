FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
# Install spark provider without its PySpark dep — Spark runs in its own container,
# we only need the Airflow operator/hook classes here.
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.8.0 --no-deps && \
    pip install --no-cache-dir -r /requirements.txt
