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

def truncate_tables():
    """Clear fact and dimension tables for a clean re-run."""
    import psycopg2
    conn = psycopg2.connect(
        host=WAREHOUSE_HOST, port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB, user=WAREHOUSE_USER, password=WAREHOUSE_PASSWORD
    )
    cur = conn.cursor()
    # Order matters: facts first (they reference dims), then dims
    tables = ["fact_transactions", "fact_market_indices", "dim_property", "dim_location"]
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
        print(f"  Truncated {table}")
    conn.commit()
    cur.close()
    conn.close()

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
# ETL: City Timeseries → fact_market_indices
# ============================================

def etl_timeseries(spark: SparkSession):
    """
    Read city timeseries JSONs from MinIO, transform, and load into fact_market_indices.
    """
    print("\n[ETL] Reading timeseries from MinIO...")

    ts_path = "s3a://nadlanist-raw/dirobot/timeseries/*/year=*/month=*/data.json"
    raw_df = spark.read.option("multiline", "true").json(ts_path)

    # timeSeriesByRooms is a STRUCT with known field names, not a MAP
    # Use stack() to unpivot the 6 room categories into rows
    rooms_df = raw_df.select(
        F.col("city"),
        F.expr("""
            stack(6,
                '1-2 rooms', timeSeriesByRooms.`1-2 חדרים`,
                '3 rooms', timeSeriesByRooms.`3 חדרים`,
                '4 rooms', timeSeriesByRooms.`4 חדרים`,
                '5 rooms', timeSeriesByRooms.`5 חדרים`,
                '6 rooms', timeSeriesByRooms.`6 חדרים`,
                '6+ rooms', timeSeriesByRooms.`6+ חדרים`
            ) as (room_category, monthly_data)
        """)
    )

    # Now explode the monthly_data array (each room category has an array of months)
    monthly_df = rooms_df.select(
        F.col("city"),
        F.col("room_category"),
        F.explode("monthly_data").alias("data")
    )

    # Flatten into columns
    flat_df = monthly_df.select(
        F.col("city"),
        F.col("room_category"),
        F.col("data.year").cast(T.IntegerType()).alias("year"),
        F.col("data.month").alias("month_name"),
        F.col("data.marketValue").cast(T.FloatType()).alias("market_value"),
        F.col("data.askingPrice").cast(T.FloatType()).alias("asking_price"),
        F.col("data.growth").cast(T.FloatType()).alias("growth_pct"),
        F.col("data.transactionCount").cast(T.IntegerType()).alias("transaction_count"),
    )

    flat_df = flat_df.filter(F.col("market_value").isNotNull())

    total = flat_df.count()
    print(f"  Total timeseries records: {total}")
    flat_df.show(5, truncate=False)

    # Write to fact_market_indices
    indices_df = flat_df.select(
        F.col("market_value").alias("avg_price_per_sqm"),
        F.col("growth_pct").alias("boi_interest_rate"),
    )
    write_to_postgres(indices_df, "fact_market_indices")

    print(f"\n[ETL] Timeseries complete! {total} records loaded.")


# ============================================
# MAIN
# ============================================

def main():
    """Run the full ETL pipeline."""
    print("=" * 60)
    print("Nadlanist -- PySpark ETL")
    print("=" * 60)

    print("\n[ETL] Cleaning tables for fresh load...")
    truncate_tables()

    spark = create_spark_session()

    try:
        etl_street_deals(spark)
        etl_timeseries(spark)
    finally:
        spark.stop()
        print("\nSpark session closed.")


if __name__ == "__main__":
    main()