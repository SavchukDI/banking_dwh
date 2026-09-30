-- Инкрементальная загрузка не должна терять транзакции
select stg_rows, fact_rows
from (
    select
        (select count() from {{ ref('stg_transactions') }}) as stg_rows,
        (select count() from {{ ref('fact_transactions') }}) as fact_rows
)
where stg_rows != fact_rows