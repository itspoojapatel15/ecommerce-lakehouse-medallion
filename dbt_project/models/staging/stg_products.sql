{{ config(materialized='view') }}

select
    product_id,
    title as product_name,
    description,
    price,
    category,
    rating,
    review_count,
    _loaded_at
from {{ source('silver', 'products') }}
