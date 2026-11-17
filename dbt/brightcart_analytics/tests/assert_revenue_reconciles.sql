-- Publication gate: the mart must hold exactly the same revenue as the
-- completed orders it was built from. Any row returned here fails the test.
with mart as (
    select sum(completed_revenue_gbp) as revenue from {{ ref('fct_daily_sales') }}
),
source_rows as (
    select sum(line_revenue_gbp) as revenue from {{ ref('int_completed_orders') }}
)
select mart.revenue as mart_revenue, source_rows.revenue as source_revenue
from mart, source_rows
where abs(mart.revenue - source_rows.revenue) > 0.01
