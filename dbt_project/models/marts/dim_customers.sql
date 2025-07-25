{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),
order_stats as (
    select
        customer_id,
        count(*) as lifetime_orders,
        sum(total_amount) as lifetime_spend,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        avg(total_amount) as avg_order_value
    from {{ ref('stg_orders') }}
    where status = 'completed'
    group by customer_id
)
select
    c.customer_id,
    c.full_name,
    c.email,
    c.segment,
    c.created_at as customer_since,
    datediff(current_date(), c.created_at) as tenure_days,
    coalesce(o.lifetime_orders, 0) as lifetime_orders,
    coalesce(round(o.lifetime_spend, 2), 0) as lifetime_spend,
    coalesce(round(o.avg_order_value, 2), 0) as avg_order_value,
    o.first_order_date,
    o.last_order_date,
    case
        when o.lifetime_spend > 1000 then 'high_value'
        when o.lifetime_spend > 500 then 'medium_value'
        when o.lifetime_spend > 0 then 'low_value'
        else 'no_purchases'
    end as value_segment,
    case
        when datediff(current_date(), o.last_order_date) <= 30 then 'active'
        when datediff(current_date(), o.last_order_date) <= 90 then 'at_risk'
        when datediff(current_date(), o.last_order_date) <= 180 then 'lapsed'
        else 'churned'
    end as activity_status
from customers c
left join order_stats o on c.customer_id = o.customer_id
