{{ config(materialized='view') }}

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    line_total,
    _loaded_at
from {{ source('silver', 'order_items') }}
