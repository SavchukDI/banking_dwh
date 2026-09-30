-- У каждого клиента должна быть ровно одна текущая версия
select
    customer_id,
    countIf(is_current) as current_versions
from {{ ref('dim_customers') }}
group by customer_id
having current_versions != 1