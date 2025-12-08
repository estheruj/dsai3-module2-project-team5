SELECT
    order_id,
    COALESCE(DATE_DIFF(order_approved_at,order_purchase_timestamp,DAY),0) AS approval_lead_time_days,

    -- 2. Carrier Lead Time
    COALESCE(
        DATE_DIFF(
            order_delivered_carrier_date,
            order_approved_at,
            DAY
        ),
        0
    ) AS carrier_lead_time_days,

    -- 3. Delivery Lead Time
    COALESCE(
        DATE_DIFF(
            order_delivered_customer_date,
            order_delivered_carrier_date,
            DAY
        ),
        0
    ) AS delivery_lead_time_days,

    -- 4. Delivery Delay Days
    -- This calculates (Actual Delivery Date - Estimated Delivery Date). A positive value means late.
    COALESCE(
        DATE_DIFF(
            order_delivered_customer_date,
            order_estimated_delivery_date,
            DAY
        ),
        0
    ) AS delivery_delay_days,

    -- 5. Delivery Is Late Flag
    -- Uses a sub-CASE to handle the NULL condition where the order hasn't been delivered yet.
    CASE
        -- If the delivery date is NULL, the order is not yet late (FALSE)
        WHEN order_delivered_customer_date IS NULL THEN FALSE
        -- If the delivery date is present, check if it's after the estimated date
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN TRUE
        ELSE FALSE
    END AS delivery_is_late

FROM
    {{ ref('stg_orders') }}