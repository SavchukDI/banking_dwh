with latest_events as (
    select *
    from {{ source('raw', 'transactions') }}
    order by transaction_id, _lsn desc, _kafka_offset desc
    limit 1 by transaction_id
)

select
    transaction_id,
    account_id,
    related_account_id,
    transaction_type,
    amount,
    currency,
    status,
    created_at as transaction_at
from latest_events
where _op != 'd'