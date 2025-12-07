"""
Silver Layer Transformation — cleans, deduplicates, enforces schema,
and applies SCD Type 2 on customer dimension.

Reads from Bronze Delta tables and writes to Silver Delta tables with:
- Type casting and validation
- Deduplication via row_number
- SCD Type 2 for slowly changing dimensions (customers)
- Data quality metrics logging
"""

from datetime import datetime, timezone

import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    trim,
    lower,
    concat_ws,
    lit,
    current_timestamp,
    row_number,
    when,
    coalesce,
)
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip, DeltaTable

from config.settings import settings

logger = structlog.get_logger()


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def bronze_path(table: str) -> str:
    return f"{settings.delta_lake_path}/bronze/{table}"


def silver_path(table: str) -> str:
    return f"{settings.delta_lake_path}/silver/{table}"


def transform_orders(spark: SparkSession) -> int:
    """Clean and deduplicate orders."""
    logger.info("transforming_orders")

    bronze = spark.read.format("delta").load(bronze_path("orders"))

    # Dedup: keep latest ingestion per order_id
    w = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())
    deduped = bronze.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    silver = deduped.select(
        col("order_id"),
        col("customer_id"),
        to_date(col("order_date")).alias("order_date"),
        trim(lower(col("status"))).alias("status"),
        col("total_amount").cast("double").alias("total_amount"),
        trim(col("shipping_address")).alias("shipping_address"),
        trim(col("payment_method")).alias("payment_method"),
        current_timestamp().alias("_loaded_at"),
    ).filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & (col("total_amount") >= 0)
    )

    silver.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path("orders"))

    count = silver.count()
    logger.info("orders_transformed", rows=count)
    return count


def transform_order_items(spark: SparkSession) -> int:
    """Clean and deduplicate order items."""
    logger.info("transforming_order_items")

    bronze = spark.read.format("delta").load(bronze_path("order_items"))

    w = Window.partitionBy("order_item_id").orderBy(col("_ingested_at").desc())
    deduped = bronze.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    silver = deduped.select(
        col("order_item_id"),
        col("order_id"),
        col("product_id"),
        col("quantity").cast("int").alias("quantity"),
        col("unit_price").cast("double").alias("unit_price"),
        coalesce(col("discount").cast("double"), lit(0.0)).alias("discount"),
        current_timestamp().alias("_loaded_at"),
    ).filter(
        col("order_item_id").isNotNull()
        & col("order_id").isNotNull()
        & (col("quantity") > 0)
        & (col("unit_price") > 0)
    )

    # Computed columns
    silver = silver.withColumn(
        "line_total",
        (col("unit_price") * col("quantity") * (1 - col("discount"))).cast("double"),
    )

    silver.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path("order_items"))

    count = silver.count()
    logger.info("order_items_transformed", rows=count)
    return count


def transform_products(spark: SparkSession) -> int:
    """Clean and deduplicate products."""
    logger.info("transforming_products")

    bronze = spark.read.format("delta").load(bronze_path("products"))

    w = Window.partitionBy("product_id").orderBy(col("_ingested_at").desc())
    deduped = bronze.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    silver = deduped.select(
        col("product_id"),
        trim(col("title")).alias("title"),
        trim(col("description")).alias("description"),
        col("price").cast("double").alias("price"),
        trim(lower(col("category"))).alias("category"),
        col("image_url"),
        col("rating_rate").cast("double").alias("rating"),
        col("rating_count").cast("int").alias("review_count"),
        current_timestamp().alias("_loaded_at"),
    ).filter(col("product_id").isNotNull() & (col("price") > 0))

    silver.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path("products"))

    count = silver.count()
    logger.info("products_transformed", rows=count)
    return count


def transform_customers_scd2(spark: SparkSession) -> int:
    """
    Apply SCD Type 2 on customers dimension.

    Tracks changes in segment, email, phone by closing old records
    (setting valid_to and is_current=false) and inserting new versions.
    """
    logger.info("transforming_customers_scd2")

    bronze = spark.read.format("delta").load(bronze_path("customers"))

    # Dedup incoming: keep latest per customer_id
    w = Window.partitionBy("customer_id").orderBy(col("_ingested_at").desc())
    incoming = (
        bronze.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            col("customer_id"),
            trim(lower(col("email"))).alias("email"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
            col("phone"),
            to_timestamp(col("created_at")).alias("created_at"),
            to_timestamp(col("updated_at")).alias("updated_at"),
            trim(col("segment")).alias("segment"),
        )
        .filter(col("customer_id").isNotNull())
    )

    target_path = silver_path("customers")

    try:
        target_table = DeltaTable.forPath(spark, target_path)

        # Merge: update existing current records if changed, insert new
        target_table.alias("target").merge(
            incoming.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true",
        ).whenMatchedUpdate(
            condition=(
                "target.email != source.email OR "
                "target.segment != source.segment OR "
                "target.phone != source.phone"
            ),
            set={
                "is_current": lit(False),
                "valid_to": current_timestamp(),
                "_loaded_at": current_timestamp(),
            },
        ).whenNotMatchedInsert(
            values={
                "customer_id": col("source.customer_id"),
                "email": col("source.email"),
                "first_name": col("source.first_name"),
                "last_name": col("source.last_name"),
                "full_name": col("source.full_name"),
                "phone": col("source.phone"),
                "created_at": col("source.created_at"),
                "updated_at": col("source.updated_at"),
                "segment": col("source.segment"),
                "is_current": lit(True),
                "valid_from": current_timestamp(),
                "valid_to": lit(None).cast("timestamp"),
                "_loaded_at": current_timestamp(),
            }
        ).execute()

        # Insert new versions for updated records
        updated = (
            incoming.alias("s")
            .join(
                spark.read.format("delta").load(target_path).filter(
                    (col("is_current") == False)
                    & (col("valid_to").isNotNull())
                ).alias("closed"),
                (col("s.customer_id") == col("closed.customer_id")),
            )
            .select(
                col("s.customer_id"),
                col("s.email"),
                col("s.first_name"),
                col("s.last_name"),
                col("s.full_name"),
                col("s.phone"),
                col("s.created_at"),
                col("s.updated_at"),
                col("s.segment"),
                lit(True).alias("is_current"),
                current_timestamp().alias("valid_from"),
                lit(None).cast("timestamp").alias("valid_to"),
                current_timestamp().alias("_loaded_at"),
            )
        )

        updated.write.format("delta").mode("append").save(target_path)

    except Exception:
        # First run — no existing table
        logger.info("creating_initial_customers_table")
        initial = incoming.withColumn("is_current", lit(True)).withColumn(
            "valid_from", current_timestamp()
        ).withColumn("valid_to", lit(None).cast("timestamp")).withColumn(
            "_loaded_at", current_timestamp()
        )
        initial.write.format("delta").mode("overwrite").save(target_path)

    count = spark.read.format("delta").load(target_path).count()
    logger.info("customers_scd2_complete", total_records=count)
    return count


def run_silver_transform():
    """Run all Silver layer transformations."""
    spark = create_spark_session()
    logger.info("silver_transform_started")

    results = {
        "orders": transform_orders(spark),
        "order_items": transform_order_items(spark),
        "products": transform_products(spark),
        "customers": transform_customers_scd2(spark),
    }

    logger.info("silver_transform_complete", results=results)
    spark.stop()
    return results


if __name__ == "__main__":
    run_silver_transform()
