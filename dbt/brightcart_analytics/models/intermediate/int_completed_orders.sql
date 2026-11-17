-- The business rule: only COMPLETED orders count as revenue.
select *
from {{ ref('stg_orders') }}
where status = 'COMPLETED'
