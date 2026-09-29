{{ config(materialized='table') }}

select
    user_id,
    txn_date,
    count(*)                                  as completed_txn_count,
    sum(amount_usd)                           as total_amount_usd,
    avg(amount_usd)                           as avg_amount_usd,
    max(amount_usd)                           as max_amount_usd
from {{ ref('stg_transactions') }}
where txn_status = 'COMPLETED'
group by
    user_id,
    txn_date
