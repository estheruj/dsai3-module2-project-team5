select
    I.order_id,
    I.order_item_id,
    O.customer_id,
    I.product_id,
    I.seller_id,
    O.order_purchase_timestamp,
    I.shipping_limit_date,
    O.order_delivered_carrier_date,
    O.order_delivered_customer_date
from {{ ref('stg_order_items') }} I
left join {{ ref('stg_orders') }} O 
    on O.order_id = I.order_id