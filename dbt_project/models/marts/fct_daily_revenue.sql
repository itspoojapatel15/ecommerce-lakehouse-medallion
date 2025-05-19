{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('fct_orders') }}
    where is_completed = true
)
select
    order_date,
    count(*) as order_count,
    count(distinct customer_id) as unique_customers,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_order_value,
    sum(item_count) as total_items_sold,
    round(avg(avg_discount), 4) as avg_discount_rate,
    round(sum(total_amount) / nullif(count(distinct customer_id), 0), 2) as revenue_per_customer
from orders
group by order_date
