{{ config(order_by='(account_id, valid_from)') }}

with versions as (
    select
        *,
        row_number() over (partition by account_id order by dbt_valid_from) as version_number
    from {{ ref('accounts_snapshot') }}
)

select
    dbt_scd_id                                          as account_version_key,
    account_id,
    customer_id,
    account_type,
    currency,
    status,
    opened_at,
    if(
        version_number = 1,
        opened_at,
        toDateTime64(dbt_valid_from, 6, 'UTC')
    )                                                   as valid_from,
    CAST(dbt_valid_to AS Nullable(DateTime64(6, 'UTC'))) as valid_to,
    dbt_valid_to is null                                as is_current,
    version_number
from versions