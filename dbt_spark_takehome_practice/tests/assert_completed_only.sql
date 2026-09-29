-- Singular dbt test.
-- A dbt test passes when this query returns ZERO rows.

select f.txn_id
from {{ ref('fct_suspicious_transactions') }} f
join {{ ref('stg_transactions') }} s
  on f.txn_id = s.txn_id
where s.txn_status <> 'COMPLETED'
