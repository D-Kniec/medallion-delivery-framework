FROM apache/airflow:2.8.1
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install pyspark apache-airflow-providers-apache-spark