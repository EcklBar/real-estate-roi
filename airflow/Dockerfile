# Custom Airflow image with Java, PySpark, and project dependencies
FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir \
    requests \
    minio \
    python-dotenv \
    pyspark==3.5.4 \
    psycopg2-binary \
    curl_cffi \
    beautifulsoup4 \
    confluent-kafka
