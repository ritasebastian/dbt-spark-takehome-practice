{{ config(materialized='view') }}

with source_data as (

    select *
    from {{ source('raw', 'transactions') }}

),

cleaned as (

    select
        trim(txn_id)                                      as txn_id,
        trim(user_id)                                     as user_id,
        trim(merchant_id)                                 as merchant_id,
        upper(trim(merchant_name))                        as merchant_name,
        cast(txn_timestamp as timestamp)                  as txn_timestamp,
        to_date(txn_timestamp)                            as txn_date,
        cast(amount_usd as decimal(18, 2))                as amount_usd,
        upper(trim(currency))                             as currency,
        upper(trim(from_country))                         as from_country,
        upper(trim(to_country))                           as to_country,
        upper(trim(txn_status))                           as txn_status,
        upper(trim(payment_method))                       as payment_method,
        case
            when upper(trim(from_country)) <> upper(trim(to_country))
                then true
            else false
        end                                               as is_cross_border
    from source_data

)

select *
from cleaned
where txn_id is not null
  and user_id is not null
  and amount_usd > 0
