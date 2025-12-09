SELECT 
  {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS payment_key,
  order_id, 
  payment_sequential, 
  payment_type, 
  payment_installments, 
  payment_value 
  FROM {{ ref('stg_order_payments') }}
  