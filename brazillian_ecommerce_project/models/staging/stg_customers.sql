SELECT
    customer_id,
    customer_unique_id,
    LOWER(customer_city) AS customer_city,
    LOWER(customer_state) AS customer_state
FROM {{ source('brazilian_ecommerce', 'customers') }}
