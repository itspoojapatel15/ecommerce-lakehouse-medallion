{{ config(materialized='table') }}

-- RFM (Recency, Frequency, Monetary) segmentation
with customer_metrics as (
    select
        customer_id,
        datediff(current_date(), max(order_date)) as recency_days,
        count(*) as frequency,
        round(sum(total_amount), 2) as monetary
    from {{ ref('fct_orders') }}
    where is_completed = true
    group by customer_id
),
scored as (
    select
        *,
        ntile(5) over (order by recency_days desc) as r_score,
        ntile(5) over (order by frequency asc) as f_score,
        ntile(5) over (order by monetary asc) as m_score
    from customer_metrics
)
select
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score as rfm_total,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'champion'
        when r_score >= 3 and f_score >= 3 then 'loyal'
        when r_score >= 4 and f_score <= 2 then 'new_customer'
        when r_score <= 2 and f_score >= 3 then 'at_risk'
        when r_score <= 2 and f_score <= 2 then 'lost'
        else 'regular'
    end as rfm_segment
from scored
