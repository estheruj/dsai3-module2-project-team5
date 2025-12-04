SELECT 
I.order_id, I.order_item_id, O.customer_id, I.product_id, I.seller_id
    , O.order_purchase_timestamp, I.shipping_limit_date
    , O.order_delivered_carrier_date, O.order_delivered_customer_date
FROM {{ ref('stg_order_items') }} I
INNER JOIN {{ ref('stg_orders') }} O 
  ON O.order_id = I.order_id
INNER JOIN {{ ref('stg_customers') }} C
  ON O.customer_id = C.customer_id
INNER JOIN {{ ref('stg_products') }} P
  ON P.product_id = I.product_id
INNER JOIN {{ ref('stg_sellers') }} S
  ON S.seller_id = I.seller_id
