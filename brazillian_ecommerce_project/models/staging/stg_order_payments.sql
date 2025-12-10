SELECT
    order_id,
    SAFE_CAST(payment_sequential AS INT) AS payment_sequential,
    payment_type,
    SAFE_CAST(payment_installments AS INT) AS payment_installments,
    SAFE_CAST(payment_value AS NUMERIC) AS payment_value
FROM {{ source('brazilian_ecommerce', 'order_payments') }}
