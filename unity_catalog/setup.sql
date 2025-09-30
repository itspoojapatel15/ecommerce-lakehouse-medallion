-- Unity Catalog setup for e-commerce lakehouse
-- Run in Databricks SQL or notebook

-- Create catalog
CREATE CATALOG IF NOT EXISTS ecommerce;
USE CATALOG ecommerce;

-- Create schemas for each medallion layer
CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'Raw data landing zone - append-only, schema-on-read';

CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'Cleaned, validated, deduplicated data with SCD Type 2';

CREATE SCHEMA IF NOT EXISTS gold
    COMMENT 'Business-level aggregates - star schema for analytics';

-- Grant permissions
-- Analysts can only read Gold
GRANT USAGE ON CATALOG ecommerce TO `analysts`;
GRANT USAGE ON SCHEMA gold TO `analysts`;
GRANT SELECT ON SCHEMA gold TO `analysts`;

-- Data engineers can read/write all layers
GRANT USAGE ON CATALOG ecommerce TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA gold TO `data_engineers`;

-- Create managed tables in Bronze
CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    status STRING,
    total_amount STRING,
    shipping_address STRING,
    payment_method STRING,
    _source STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING
)
USING DELTA
COMMENT 'Raw orders from PostgreSQL CDC';

CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity STRING,
    unit_price STRING,
    discount STRING,
    _source STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING
)
USING DELTA
COMMENT 'Raw order line items';

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    phone STRING,
    created_at STRING,
    updated_at STRING,
    segment STRING,
    _source STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING
)
USING DELTA
COMMENT 'Raw customer data - used for SCD Type 2 in Silver';

CREATE TABLE IF NOT EXISTS bronze.products (
    product_id STRING,
    title STRING,
    description STRING,
    price STRING,
    category STRING,
    image_url STRING,
    rating_rate STRING,
    rating_count STRING,
    _source STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING
)
USING DELTA
COMMENT 'Raw product catalog from REST API';

-- Silver tables
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    phone STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    segment STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    _loaded_at TIMESTAMP
)
USING DELTA
COMMENT 'SCD Type 2 customer dimension'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Gold tables
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    phone STRING,
    segment STRING,
    customer_since TIMESTAMP,
    tenure_days INT,
    segment_rank INT
)
USING DELTA
COMMENT 'Current customer dimension for analytics';

CREATE TABLE IF NOT EXISTS gold.fct_orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    order_year INT,
    order_quarter INT,
    order_month INT,
    order_dow INT,
    status STRING,
    total_amount DOUBLE,
    items_total DOUBLE,
    item_count LONG,
    unique_products LONG,
    avg_discount DOUBLE,
    payment_method STRING,
    is_completed BOOLEAN,
    is_refunded BOOLEAN,
    _loaded_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (order_year, order_month)
COMMENT 'Order fact table';

CREATE TABLE IF NOT EXISTS gold.fct_daily_revenue (
    order_date DATE,
    order_count LONG,
    unique_customers LONG,
    total_revenue DOUBLE,
    avg_order_value DOUBLE,
    total_items_sold LONG,
    avg_discount_rate DOUBLE,
    revenue_per_customer DOUBLE
)
USING DELTA
COMMENT 'Daily revenue aggregation';
