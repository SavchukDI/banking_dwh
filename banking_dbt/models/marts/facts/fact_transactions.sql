{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        order_by='(transaction_date, transaction_id)',
        partition_by='toYYYYMM(transaction_date)'
    )
}}

with transactions as (
    select *
    from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    -- берём новые транзакции + окно в 1 час на случай опоздавших событий
    where transaction_at >= (select max(transaction_at) - interval 1 hour from {{ this }})
    {% endif %}
)

select
    t.transaction_id                    as transaction_id,
    t.transaction_at                    as transaction_at,
    toDate(t.transaction_at)            as transaction_date,

    t.account_id                        as account_id,
    a.account_version_key               as account_version_key,
    a.customer_id                       as customer_id,
    c.customer_version_key              as customer_version_key,
    t.related_account_id                as related_account_id,

    t.transaction_type                  as transaction_type,
    t.status                            as status,
    a.status                            as account_status_at_transaction,
    t.currency                          as currency,
    t.amount                            as amount,
    multiIf(
        t.status != 'completed',       toDecimal64(0, 2),
        t.transaction_type = 'deposit', t.amount,
        -t.amount
    )                                   as balance_impact,

    now()                               as _dbt_loaded_at
from transactions t
asof left join {{ ref('dim_accounts') }} a
    on t.account_id = a.account_id
   and t.transaction_at >= a.valid_from
asof left join {{ ref('dim_customers') }} c
    on a.customer_id = c.customer_id
   and t.transaction_at >= c.valid_from