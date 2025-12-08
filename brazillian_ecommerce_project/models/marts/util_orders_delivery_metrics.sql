SELECT
    order_id,
    COALESCE(DATE_DIFF(order_approved_at,order_purchase_timestamp,DAY),0) AS approval_lead_time_days,
    COALESCE(DATE_DIFF(order_delivered_carrier_date,order_approved_at,DAY),0) AS carrier_lead_time_days,
    COALESCE(DATE_DIFF(order_delivered_customer_date,order_delivered_carrier_date,DAY),0) AS delivery_lead_time_days,
    COALESCE(DATE_DIFF(order_delivered_customer_date,order_estimated_delivery_date,DAY),0) AS delivery_delay_days,
    CASE
        WHEN order_delivered_customer_date IS NULL THEN FALSE
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN TRUE
        ELSE FALSE
    END AS delivery_is_late

FROM
    {{ ref('stg_orders') }}