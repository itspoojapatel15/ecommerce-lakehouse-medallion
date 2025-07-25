"""
Bronze Layer Ingestion — lands raw data from all sources into Delta Lake.

Sources:
- PostgreSQL (orders, order_items, customers) via JDBC
- REST API (products) via httpx
- CSV/Parquet files via Auto Loader pattern

All bronze tables are append-only with metadata columns:
_source, _ingested_at, _batch_id
"""

import json
import uuid
from datetime import datetime, timezone

import httpx
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, col
from delta import configure_spark_with_delta_pip

from config.settings import settings

logger = structlog.get_logger()


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("BronzeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def bronze_path(table: str) -> str:
    return f"{settings.delta_lake_path}/bronze/{table}"


def ingest_from_postgres(spark: SparkSession, table: str, batch_id: str) -> int:
    """Ingest a PostgreSQL table into Bronze Delta Lake."""
    logger.info("ingesting_postgres", table=table, batch_id=batch_id)

    pg = settings.postgres
    df = (
        spark.read.format("jdbc")
        .option("url", pg.jdbc_url)
        .option("dbtable", table)
        .option("user", pg.user)
        .option("password", pg.password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Cast all columns to string for schema-on-read
    string_df = df.select([col(c).cast("string").alias(c) for c in df.columns])

    bronze_df = (
        string_df.withColumn("_source", lit("postgres"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )

    bronze_df.write.format("delta").mode("append").save(bronze_path(table))

    row_count = bronze_df.count()
    logger.info("postgres_ingested", table=table, rows=row_count)
    return row_count


def ingest_from_api(spark: SparkSession, batch_id: str) -> int:
    """Ingest products from REST API into Bronze Delta Lake."""
    logger.info("ingesting_api", url=settings.product_api_url)

    response = httpx.get(settings.product_api_url, timeout=30)
    response.raise_for_status()
    products = response.json()

    rows = []
    for p in products:
        rows.append(
            {
                "product_id": str(p["id"]),
                "title": p.get("title", ""),
                "description": p.get("description", ""),
                "price": str(p.get("price", "")),
                "category": p.get("category", ""),
                "image_url": p.get("image", ""),
                "rating_rate": str(p.get("rating", {}).get("rate", "")),
                "rating_count": str(p.get("rating", {}).get("count", "")),
            }
        )

    df = spark.createDataFrame(rows)
    bronze_df = (
        df.withColumn("_source", lit("rest_api"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )

    bronze_df.write.format("delta").mode("append").save(bronze_path("products"))

    logger.info("api_ingested", products=len(rows))
    return len(rows)


def ingest_csv_files(
    spark: SparkSession, file_path: str, table: str, batch_id: str
) -> int:
    """Ingest CSV files into Bronze Delta Lake (Auto Loader pattern)."""
    logger.info("ingesting_csv", path=file_path, table=table)

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(file_path)

    string_df = df.select([col(c).cast("string").alias(c) for c in df.columns])

    bronze_df = (
        string_df.withColumn("_source", lit("csv_upload"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )

    bronze_df.write.format("delta").mode("append").save(bronze_path(table))

    row_count = bronze_df.count()
    logger.info("csv_ingested", table=table, rows=row_count)
    return row_count


def run_bronze_ingestion():
    """Run full Bronze layer ingestion from all sources."""
    spark = create_spark_session()
    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    logger.info("bronze_ingestion_started", batch_id=batch_id)

    results = {}

    # PostgreSQL tables
    for table in ["orders", "order_items", "customers"]:
        try:
            results[table] = ingest_from_postgres(spark, table, batch_id)
        except Exception as e:
            logger.error("ingestion_failed", table=table, error=str(e))
            results[table] = {"error": str(e)}

    # REST API products
    try:
        results["products"] = ingest_from_api(spark, batch_id)
    except Exception as e:
        logger.error("api_ingestion_failed", error=str(e))
        results["products"] = {"error": str(e)}

    logger.info("bronze_ingestion_complete", results=results)
    spark.stop()
    return results


if __name__ == "__main__":
    run_bronze_ingestion()
