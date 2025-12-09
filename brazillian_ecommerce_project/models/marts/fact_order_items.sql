SELECT 
  {{ dbt_utils.generate_surrogate_key(['order_id'])}} AS order_item_key,
  order_id, 
  order_item_id, 
  product_id, 
  seller_id, 
  shipping_limit_date, 
  price, 
  freight_value ,
  price + freight_value AS gross_item_value
FROM {{ ref('stg_order_items') }}
