{{ config(enabled=(target.name == 'ci'), tags=['ci']) }}

-- Ожидаемое состояние staging после двух фаз фикстур
select 'stg_customers: expected 2 rows (customer 3 deleted)' as failure
where (select count() from {{ ref('stg_customers') }}) != 2

union all
select 'stg_customers: customer 1 should have the latest phone'
where (select any(phone) from {{ ref('stg_customers') }} where customer_id = 1) != '+7 900 000 00 99'

union all
select 'stg_customers: customer 2 should have the latest last_name'
where (select any(last_name) from {{ ref('stg_customers') }} where customer_id = 2) != 'Volkova'

union all
select 'stg_accounts: account 10 balance should be 1150.00 (ordering by _lsn)'
where (select any(balance) from {{ ref('stg_accounts') }} where account_id = 10) != toDecimal64(1150, 2)

union all
select 'stg_accounts: account 11 should be blocked with balance 450.00'
where (
    select any(tuple(status, balance)) from {{ ref('stg_accounts') }} where account_id = 11
) != tuple('blocked', toDecimal64(450, 2))

union all
select 'stg_transactions: expected 4 unique transactions (duplicates removed)'
where (select count() from {{ ref('stg_transactions') }}) != 4