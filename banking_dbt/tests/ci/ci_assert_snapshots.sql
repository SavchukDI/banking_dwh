{{ config(enabled=(target.name == 'ci'), tags=['ci']) }}

-- SCD2: новые версии только при изменении отслеживаемых атрибутов
select 'accounts_snapshot: account 11 should have 2 versions (active -> blocked)' as failure
where (select count() from {{ ref('accounts_snapshot') }} where account_id = 11) != 2

union all
select 'accounts_snapshot: account 10 should have 1 version (balance is not tracked)'
where (select count() from {{ ref('accounts_snapshot') }} where account_id = 10) != 1

union all
select 'customers_snapshot: customer 2 should have 2 versions (last_name changed)'
where (select count() from {{ ref('customers_snapshot') }} where customer_id = 2) != 2

union all
select 'customers_snapshot: customer 1 should have 1 version (change happened before first snapshot)'
where (select count() from {{ ref('customers_snapshot') }} where customer_id = 1) != 1