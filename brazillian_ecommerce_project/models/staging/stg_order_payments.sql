SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    SAFE_CAST(payment_value AS NUMERIC) AS payment_value
FROM {{ source('brazilian_ecommerce', 'order_payments') }}
