"""
etl_transactions.py -- PySpark ETL
Read raw JSON from MinIO, transform, load into PostgreSQL Star Schema.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

load_dotenv()

# ============================================
# CONFIG
# ============================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "nadlanist")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "nadlanist")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "nadlanist123")

JDBC_URL = f"jdbc:postgresql://{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
JDBC_PROPERTIES = {
    "user": WAREHOUSE_USER,
    "password": WAREHOUSE_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ============================================
# SPARK SESSION
# ============================================

def create_spark_session() -> SparkSession:
    """Create SparkSession configured for MinIO (S3A) and PostgreSQL (JDBC)."""
    jar_path = os.path.join(os.path.dirname(__file__), "..", "jars", "postgresql-42.7.1.jar")

    spark = (
        SparkSession.builder
        .appName("Nadlanist-ETL")
        .master("local[*]")
        .config("spark.jars", jar_path)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark