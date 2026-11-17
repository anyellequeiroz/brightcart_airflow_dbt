-- Latest known version of each customer (the most recent daily snapshot).
with ranked as (
    select
        cast(customer_id as integer) as customer_id,
        customer_name,
        city,
        email,
        run_date,
        row_number() over (partition by customer_id order by run_date desc) as rn
    from {{ source('raw', 'customers') }}
)
select customer_id, customer_name, city, email, run_date as snapshot_date
from ranked
where rn = 1
