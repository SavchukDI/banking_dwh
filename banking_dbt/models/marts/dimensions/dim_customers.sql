{{ config(order_by='(customer_id, valid_from)') }}

with versions as (
    select
        *,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_number
    from {{ ref('customers_snapshot') }}
)

select
    dbt_scd_id                                          as customer_version_key,
    customer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name)                  as full_name,
    email,
    phone,
    date_of_birth,
    created_at,
    -- первая версия действует с момента создания клиента
    if(
        version_number = 1,
        created_at,
        toDateTime64(dbt_valid_from, 6, 'UTC')
    )                                                   as valid_from,
    CAST(dbt_valid_to AS Nullable(DateTime64(6, 'UTC'))) as valid_to,
    dbt_valid_to is null                                as is_current,
    version_number
from versions