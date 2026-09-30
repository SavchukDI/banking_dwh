{{ config(enabled=(target.name == 'ci'), tags=['ci']) }}

-- Точные ожидания по каждой транзакции:
-- balance_impact и версия клиента, найденная ASOF JOIN'ом
with expected as (
    select 1000 as transaction_id, toDecimal64(200, 2)  as balance_impact, 1 as customer_version
    union all select 1001, toDecimal64(-100, 2), 1
    union all select 1002, toDecimal64(0, 2),    1   -- failed: влияния на баланс нет
    union all select 1003, toDecimal64(-50, 2),  2   -- после смены фамилии в 07:00
),

actual as (
    select
        f.transaction_id                  as transaction_id,
        f.balance_impact                  as balance_impact,
        c.version_number                  as customer_version,
        f.account_status_at_transaction   as account_status
    from {{ ref('fact_transactions') }} f
    left join {{ ref('dim_customers') }} c
        on f.customer_version_key = c.customer_version_key
)

select
    e.transaction_id      as transaction_id,
    e.balance_impact      as expected_impact,
    a.balance_impact      as actual_impact,
    e.customer_version    as expected_customer_version,
    a.customer_version    as actual_customer_version,
    a.account_status      as account_status
from expected e
left join actual a on e.transaction_id = a.transaction_id
where a.balance_impact   != e.balance_impact
   or a.customer_version != e.customer_version
   or a.account_status   != 'active'