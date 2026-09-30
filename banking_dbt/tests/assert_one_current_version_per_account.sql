-- У каждого счёта должна быть ровно одна текущая версия
select
    account_id,
    countIf(is_current) as current_versions
from {{ ref('dim_accounts') }}
group by account_id
having current_versions != 1