-- Completed revenue per day and city: the table the operations team reads.
select
    order_date,
    city,
    count(*)              as completed_orders,
    sum(line_revenue_gbp) as completed_revenue_gbp
from {{ ref('int_completed_orders') }}
group by order_date, city
