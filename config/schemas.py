"""Delta Lake table schemas for each medallion layer."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
    BooleanType,
    LongType,
)

# --- Bronze Schemas (schema-on-read, stored as raw) ---

BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_batch_id", StringType(), True),
    ]
)

BRONZE_ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_batch_id", StringType(), True),
    ]
)

BRONZE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_batch_id", StringType(), True),
    ]
)

BRONZE_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", StringType(), True),
        StructField("category", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("rating_rate", StringType(), True),
        StructField("rating_count", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_batch_id", StringType(), True),
    ]
)

# --- Silver Schemas (enforced types, cleaned) ---

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", DateType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("_loaded_at", TimestampType(), True),
    ]
)

SILVER_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("segment", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("valid_from", TimestampType(), True),
        StructField("valid_to", TimestampType(), True),
        StructField("_loaded_at", TimestampType(), True),
    ]
)
