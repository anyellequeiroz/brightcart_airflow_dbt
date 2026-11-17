-- One row per order, with clean types and names.
select
    cast(order_id as integer)              as order_id,
    cast(customer_id as integer)           as customer_id,
    cast(order_date as date)               as order_date,
    product_name,
    cast(quantity as integer)              as quantity,
    cast(unit_price_gbp as decimal(10, 2)) as unit_price_gbp,
    upper(status)                          as status,
    city,
    cast(line_revenue_gbp as decimal(10, 2)) as line_revenue_gbp,
    run_date
from {{ source('raw', 'orders') }}
