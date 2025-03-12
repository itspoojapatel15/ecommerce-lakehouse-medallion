{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    shipping_address,
    payment_method,
    _loaded_at
from {{ source('silver', 'orders') }}
