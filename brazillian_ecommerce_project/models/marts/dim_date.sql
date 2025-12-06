with bounds as (
    select
        date(min(order_purchase_timestamp)) as min_date,
        date(max(order_purchase_timestamp)) as max_date
    from {{ ref('stg_orders') }}
),

calendar as (
    select
        day as date
    from bounds,
    unnest(generate_date_array(min_date, max_date)) as day
)

select
    cast(format_date('%Y%m%d', date) as int64) as date_key,
    date,
    extract(day      from date) as day,
    extract(month    from date) as month,
    extract(year     from date) as year,
    extract(isoweek  from date) as week_of_year,
    extract(quarter  from date) as quarter
from calendar