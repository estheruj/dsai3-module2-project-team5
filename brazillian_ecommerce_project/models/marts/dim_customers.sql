WITH location AS(
  SELECT location_key, zip_prefix, city, state, latitude, longitude FROM {{ ref('dim_location') }}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY state ORDER BY location_key ASC) = 1
)

SELECT 
    C.customer_id, 
    C.customer_unique_id, 
    L.zip_prefix AS customer_zip_prefix,
    C.customer_city, 
    C.customer_state,
    L.location_key
FROM  {{ ref('stg_customers') }} C
LEFT JOIN location L
  ON C.customer_state = L.state