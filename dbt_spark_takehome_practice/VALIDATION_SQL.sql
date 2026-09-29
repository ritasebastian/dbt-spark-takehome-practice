-- 1. Raw data
select *
from raw.transactions
order by txn_timestamp;

-- 2. Staging
select *
from dbt_practice.stg_transactions
order by txn_timestamp;

-- 3. Daily metrics
select *
from dbt_practice.int_user_daily_metrics
order by user_id, txn_date;

-- 4. Suspicious transactions
select *
from dbt_practice.fct_suspicious_transactions
order by txn_timestamp;

-- 5. Check duplicate transaction IDs
select
    txn_id,
    count(*) as cnt
from dbt_practice.fct_suspicious_transactions
group by txn_id
having count(*) > 1;

-- 6. Check suspicious count by reason
select
    suspicious_reason,
    count(*) as txn_count
from dbt_practice.fct_suspicious_transactions
group by suspicious_reason
order by txn_count desc;
