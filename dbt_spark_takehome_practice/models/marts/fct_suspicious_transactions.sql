{{
    config(
        materialized='incremental',
        unique_key='txn_id',
        incremental_strategy='merge'
    )
}}

with transactions as (

    select *
    from {{ ref('stg_transactions') }}
    where txn_status = 'COMPLETED'

    {% if is_incremental() %}
      and txn_timestamp >= (
          select coalesce(
              max(txn_timestamp) - interval 1 day,
              timestamp('1900-01-01')
          )
          from {{ this }}
      )
    {% endif %}

),

metrics as (

    select *
    from {{ ref('int_user_daily_metrics') }}

),

scored as (

    select
        t.txn_id,
        t.user_id,
        t.merchant_id,
        t.merchant_name,
        t.txn_timestamp,
        t.txn_date,
        t.amount_usd,
        t.currency,
        t.from_country,
        t.to_country,
        t.payment_method,
        t.is_cross_border,

        m.completed_txn_count,
        m.avg_amount_usd,

        case when t.amount_usd >= 1000 then 1 else 0 end
            as high_amount_flag,

        case
            when t.is_cross_border = true
             and t.amount_usd >= 500
            then 1 else 0
        end as cross_border_flag,

        case
            when m.completed_txn_count >= 3
            then 1 else 0
        end as velocity_flag,

        case
            when m.avg_amount_usd > 0
             and t.amount_usd >= (3 * m.avg_amount_usd)
            then 1 else 0
        end as amount_spike_flag

    from transactions t
    inner join metrics m
        on t.user_id = m.user_id
       and t.txn_date = m.txn_date

),

final as (

    select
        *,
        case
            when high_amount_flag = 1
              or cross_border_flag = 1
              or velocity_flag = 1
              or amount_spike_flag = 1
            then 1
            else 0
        end as suspicious_flag,

        concat_ws(
            ', ',
            case when high_amount_flag = 1 then 'HIGH_AMOUNT' end,
            case when cross_border_flag = 1 then 'CROSS_BORDER_HIGH_AMOUNT' end,
            case when velocity_flag = 1 then 'HIGH_DAILY_VELOCITY' end,
            case when amount_spike_flag = 1 then 'AMOUNT_SPIKE' end
        ) as suspicious_reason

    from scored

)

select *
from final
where suspicious_flag = 1
