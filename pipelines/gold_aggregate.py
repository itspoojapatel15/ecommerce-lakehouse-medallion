"""
Gold Layer Aggregation — builds star schema fact and dimension tables
from Silver layer for analytics consumption.

Produces:
- dim_customers (current view from SCD2)
- dim_products (enriched with categories)
- dim_dates (date dimension)
- fct_orders (order-level facts with customer/product keys)
- fct_daily_revenue (daily revenue aggregation)
"""

import structlog
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    countDistinct,
    current_timestamp,
    datediff,
    to_date,
    dayofweek,
    month,
    quarter,
    year,
    when,
    round as spark_round,
    expr,
)
from delta import configure_spark_with_delta_pip

from config.settings import settings

logger = structlog.get_logger()


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("GoldAggregate")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def silver_path(table: str) -> str:
    return f"{settings.delta_lake_path}/silver/{table}"


def gold_path(table: str) -> str:
    return f"{settings.delta_lake_path}/gold/{table}"


def build_dim_customers(spark: SparkSession) -> int:
    """Current customer dimension from SCD2 silver table."""
    logger.info("building_dim_customers")

    customers = spark.read.format("delta").load(silver_path("customers"))

    dim = customers.filter(col("is_current") == True).select(
        col("customer_id"),
        col("email"),
        col("first_name"),
        col("last_name"),
        col("full_name"),
        col("phone"),
        col("segment"),
        col("created_at").alias("customer_since"),
        datediff(current_timestamp(), col("created_at")).alias("tenure_days"),
        when(col("segment") == "premium", 3)
        .when(col("segment") == "standard", 2)
        .otherwise(1)
        .alias("segment_rank"),
    )

    dim.write.format("delta").mode("overwrite").save(gold_path("dim_customers"))

    count_val = dim.count()
    logger.info("dim_customers_built", rows=count_val)
    return count_val


def build_dim_products(spark: SparkSession) -> int:
    """Product dimension with enriched categories."""
    logger.info("building_dim_products")

    products = spark.read.format("delta").load(silver_path("products"))

    dim = products.select(
        col("product_id"),
        col("title").alias("product_name"),
        col("category"),
        col("price"),
        col("rating"),
        col("review_count"),
        when(col("price") > 100, "premium")
        .when(col("price") > 50, "mid_range")
        .otherwise("budget")
        .alias("price_tier"),
        when(col("rating") >= 4.5, "excellent")
        .when(col("rating") >= 3.5, "good")
        .when(col("rating") >= 2.5, "average")
        .otherwise("poor")
        .alias("rating_tier"),
    )

    dim.write.format("delta").mode("overwrite").save(gold_path("dim_products"))

    count_val = dim.count()
    logger.info("dim_products_built", rows=count_val)
    return count_val


def build_fct_orders(spark: SparkSession) -> int:
    """Order-level fact table joining orders with items."""
    logger.info("building_fct_orders")

    orders = spark.read.format("delta").load(silver_path("orders"))
    items = spark.read.format("delta").load(silver_path("order_items"))

    order_items_agg = items.groupBy("order_id").agg(
        spark_sum("line_total").alias("items_total"),
        count("*").alias("item_count"),
        countDistinct("product_id").alias("unique_products"),
        avg("discount").alias("avg_discount"),
    )

    fct = (
        orders.join(order_items_agg, "order_id", "left")
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_date"),
            year(col("order_date")).alias("order_year"),
            quarter(col("order_date")).alias("order_quarter"),
            month(col("order_date")).alias("order_month"),
            dayofweek(col("order_date")).alias("order_dow"),
            col("status"),
            spark_round(col("total_amount"), 2).alias("total_amount"),
            spark_round(col("items_total"), 2).alias("items_total"),
            col("item_count"),
            col("unique_products"),
            spark_round(col("avg_discount"), 4).alias("avg_discount"),
            col("payment_method"),
            when(col("status") == "completed", True).otherwise(False).alias("is_completed"),
            when(col("status") == "refunded", True).otherwise(False).alias("is_refunded"),
            current_timestamp().alias("_loaded_at"),
        )
    )

    fct.write.format("delta").mode("overwrite").partitionBy("order_year", "order_month").save(
        gold_path("fct_orders")
    )

    count_val = fct.count()
    logger.info("fct_orders_built", rows=count_val)
    return count_val


def build_fct_daily_revenue(spark: SparkSession) -> int:
    """Daily revenue aggregation fact table."""
    logger.info("building_fct_daily_revenue")

    orders = spark.read.format("delta").load(gold_path("fct_orders"))

    daily = (
        orders.filter(col("is_completed") == True)
        .groupBy("order_date")
        .agg(
            count("*").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            spark_sum("item_count").alias("total_items_sold"),
            spark_round(avg("avg_discount"), 4).alias("avg_discount_rate"),
        )
        .withColumn("revenue_per_customer", spark_round(col("total_revenue") / col("unique_customers"), 2))
    )

    daily.write.format("delta").mode("overwrite").save(gold_path("fct_daily_revenue"))

    count_val = daily.count()
    logger.info("fct_daily_revenue_built", rows=count_val)
    return count_val


def run_gold_aggregation():
    """Run all Gold layer builds."""
    spark = create_spark_session()
    logger.info("gold_aggregation_started")

    results = {
        "dim_customers": build_dim_customers(spark),
        "dim_products": build_dim_products(spark),
        "fct_orders": build_fct_orders(spark),
        "fct_daily_revenue": build_fct_daily_revenue(spark),
    }

    logger.info("gold_aggregation_complete", results=results)
    spark.stop()
    return results


if __name__ == "__main__":
    run_gold_aggregation()
