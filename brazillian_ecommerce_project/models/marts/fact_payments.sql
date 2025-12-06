with payments as (
    select * 
    from {{ ref('stg_order_payments') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_purchase_timestamp
    from {{ ref('stg_orders') }}
)

select
    p.order_id,
    p.payment_sequential,

    -- FKs
    o.customer_id,
    cast(format_date('%Y%m%d', date(o.order_purchase_timestamp)) as int64)
        as payment_date_key,

    -- Attributes
    p.payment_type,
    p.payment_installments,

    -- Measures
    p.payment_value
from payments p
join orders o
    on p.order_id = o.order_id
