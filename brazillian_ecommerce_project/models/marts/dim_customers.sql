SELECT 
    C.customer_id, 
    C.customer_unique_id, 
    C.customer_zip_code_prefix AS customer_zip_prefix,
    C.customer_city, 
    C.customer_state,
    L.location_key
FROM  {{ ref('stg_customers') }} C
LEFT JOIN {{ ref('dim_location') }} L
  ON C.customer_zip_code_prefix = L.zip_prefix