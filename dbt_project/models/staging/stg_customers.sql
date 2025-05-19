{{ config(materialized='view') }}

select
    customer_id,
    email,
    first_name,
    last_name,
    full_name,
    phone,
    segment,
    created_at,
    updated_at,
    is_current,
    valid_from,
    valid_to
from {{ source('silver', 'customers') }}
where is_current = true
