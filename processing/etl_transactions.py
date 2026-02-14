"""
etl_transactions.py -- PySpark ETL
Read raw JSON from MinIO, transform, load into PostgreSQL Star Schema.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

load_dotenv()

# ============================================
# CONFIG
# ============================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "nadlanist")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD")

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
    jars_dir = os.path.join(os.path.dirname(__file__), "..", "jars")
    jar_files = ",".join([
        os.path.join(jars_dir, "postgresql-42.7.1.jar"),
        os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
        os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar"),
    ])

    spark = (
        SparkSession.builder
        .appName("Nadlanist-ETL")
        .master("local[*]")
        .config("spark.jars", jar_files)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
# ============================================
# HELPER: Write DataFrame to PostgreSQL
# ============================================

def write_to_postgres(df, table_name: str, mode: str = "append"):
    """Write a Spark DataFrame to a PostgreSQL table."""
    df.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode=mode,
        properties=JDBC_PROPERTIES,
    )
    print(f"  Wrote {df.count()} rows to {table_name}")


# ============================================
# ETL: Street Deals → Star Schema
# ============================================

def etl_street_deals(spark: SparkSession):
    """
    Read street deal JSONs from MinIO, transform, and load into
    dim_location, dim_property, and fact_transactions.
    """
    print("\n[ETL] Reading street deals from MinIO...")

    # Step 1: Read all deal files from MinIO
    deals_path = "s3a://nadlanist-raw/dirobot/deals/*/year=*/month=*/data.json"
    raw_df = spark.read.option("multiline", "true").json(deals_path)

    # The JSON has nested structure: {city, street, deals: [...]}
    # Explode the deals array so each deal becomes its own row
    deals_df = raw_df.select(
        F.col("city"),
        F.col("street"),
        F.explode("deals").alias("deal")
    )

    # Flatten the deal struct into columns
    flat_df = deals_df.select(
        F.col("city"),
        F.col("street"),
        F.col("deal.neighborhood").alias("neighborhood"),
        F.col("deal.street").alias("street_name"),
        F.col("deal.houseNumber").alias("house_number"),
        F.col("deal.propertyType").alias("property_type"),
        F.col("deal.rooms").cast(T.FloatType()).alias("rooms"),
        F.col("deal.floor").cast(T.IntegerType()).alias("floor"),
        F.col("deal.buildYear").cast(T.IntegerType()).alias("year_built"),
        F.col("deal.numberOfFloors").cast(T.IntegerType()).alias("total_floors"),
        F.col("deal.price").cast(T.FloatType()).alias("price"),
        F.col("deal.saleDate").alias("sale_date"),
    )

    # Step 2: Add calculated fields
    enriched_df = flat_df.withColumn(
        "time_id",
        F.regexp_replace(F.col("sale_date"), "-", "").cast(T.IntegerType())
    )

    # Filter out rows with no price
    enriched_df = enriched_df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    total = enriched_df.count()
    print(f"  Total deals after cleaning: {total}")
    enriched_df.show(5, truncate=False)

    # Step 3: Build and load dim_location
    print("\n[ETL] Loading dim_location...")
    locations_df = (
        enriched_df
        .select("city", "neighborhood", "street_name")
        .withColumnRenamed("street_name", "street")
        .distinct()
    )
    write_to_postgres(locations_df, "dim_location")

    # Step 4: Build and load dim_property
    print("\n[ETL] Loading dim_property...")
    properties_df = (
        enriched_df
        .select("property_type", "rooms", "year_built", "floor", "total_floors")
        .distinct()
    )
    write_to_postgres(properties_df, "dim_property")

    # Step 5: Load fact_transactions
    print("\n[ETL] Loading fact_transactions...")
    facts_df = enriched_df.select(
        F.col("time_id"),
        F.col("price"),
        F.col("property_type").alias("transaction_type"),
        F.concat(
            F.col("city"), F.lit(", "),
            F.col("street_name"), F.lit(" "),
            F.col("house_number")
        ).alias("raw_address"),
    )
    write_to_postgres(facts_df, "fact_transactions")

    print(f"\n[ETL] Street deals complete! {total} transactions loaded.")


# ============================================
# MAIN
# ============================================

def main():
    """Run the full ETL pipeline."""
    print("=" * 60)
    print("Nadlanist -- PySpark ETL")
    print("=" * 60)

    spark = create_spark_session()

    try:
        etl_street_deals(spark)
    finally:
        spark.stop()
        print("\nSpark session closed.")


if __name__ == "__main__":
    main()