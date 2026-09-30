{% snapshot accounts_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='account_id',
        strategy='check',
        check_cols=['status', 'account_type', 'currency']
    )
}}

select
    account_id,
    customer_id,
    account_type,
    currency,
    status,
    opened_at
from {{ ref('stg_accounts') }}

{% endsnapshot %}