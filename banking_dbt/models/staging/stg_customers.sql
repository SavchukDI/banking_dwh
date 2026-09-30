with latest_events as (
    select *
    from {{ source('raw', 'customers') }}
    order by customer_id, _lsn desc, _kafka_offset desc
    limit 1 by customer_id
)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    created_at,
    updated_at,
    _source_ts as last_changed_at
from latest_events
where _op != 'd'