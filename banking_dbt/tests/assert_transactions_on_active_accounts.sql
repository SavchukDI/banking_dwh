{{ config(severity='warn') }}

-- Операции по заблокированным и закрытым счетам проходить не должны
select
    transaction_id,
    account_id,
    transaction_at,
    account_status_at_transaction
from {{ ref('fact_transactions') }}
where account_status_at_transaction != 'active'