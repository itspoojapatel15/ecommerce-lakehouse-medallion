{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_products') }}
),
sales as (
    select
        product_id,
        sum(quantity) as total_units_sold,
        sum(line_total) as total_revenue,
        count(distinct order_id) as order_count
    from {{ ref('stg_order_items') }}
    group by product_id
)
select
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.rating,
    p.review_count,
    case
        when p.price > 100 then 'premium'
        when p.price > 50 then 'mid_range'
        else 'budget'
    end as price_tier,
    coalesce(s.total_units_sold, 0) as total_units_sold,
    coalesce(round(s.total_revenue, 2), 0) as total_revenue,
    coalesce(s.order_count, 0) as order_count
from products p
left join sales s on p.product_id = s.product_id
