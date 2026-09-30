with latest_events as (
    select *
    from {{ source('raw', 'accounts') }}
    order by account_id, _lsn desc, _kafka_offset desc
    limit 1 by account_id
)

select
    account_id,
    customer_id,
    account_type,
    currency,
    balance,
    status,
    opened_at,
    updated_at,
    _source_ts as last_changed_at
from latest_events
where _op != 'd'