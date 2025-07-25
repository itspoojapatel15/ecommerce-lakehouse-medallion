{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={'field': 'order_date', 'data_type': 'date', 'granularity': 'month'}
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),
items_agg as (
    select
        order_id,
        sum(line_total) as items_total,
        count(*) as item_count,
        count(distinct product_id) as unique_products,
        avg(discount) as avg_discount
    from {{ ref('stg_order_items') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.order_date,
    year(o.order_date) as order_year,
    quarter(o.order_date) as order_quarter,
    month(o.order_date) as order_month,
    dayofweek(o.order_date) as order_dow,
    o.status,
    round(o.total_amount, 2) as total_amount,
    round(i.items_total, 2) as items_total,
    i.item_count,
    i.unique_products,
    round(i.avg_discount, 4) as avg_discount,
    o.payment_method,
    o.status = 'completed' as is_completed,
    o.status = 'refunded' as is_refunded,
    current_timestamp() as _loaded_at
from orders o
left join items_agg i on o.order_id = i.order_id
{% if is_incremental() %}
where o._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
