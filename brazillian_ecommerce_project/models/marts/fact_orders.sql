SELECT 
  O.order_id, 
  O.customer_id, 
  O.order_status, 
  O.order_purchase_timestamp AS purchase_ts, 
  O.order_approved_at AS approved_ts, 
  O.order_delivered_carrier_date AS delivered_carrier_ts, 
  O.order_delivered_customer_date AS delivered_customer_ts, 
  O.order_estimated_delivery_date AS estimated_delivery_ts,
  UNIX_SECONDS(O.order_purchase_timestamp) AS purchase_date_key,
  UNIX_SECONDS(O.order_delivered_customer_date) AS delivered_date_key,
  DATE_DIFF(O.order_approved_at, O.order_purchase_timestamp, DAY) AS approval_lead_time_days,
  DATE_DIFF(O.order_delivered_carrier_date, O.order_approved_at, DAY) AS carrier_lead_time_days,
  DATE_DIFF(O.order_delivered_customer_date, O.order_purchase_timestamp, DAY) AS delivery_lead_time_days,
  DATE_DIFF(O.order_delivered_customer_date, O.order_estimated_delivery_date, DAY) AS delivery_delay_days,
  U.delivery_is_late
  FROM {{ ref('stg_orders') }} O
  LEFT JOIN {{ ref('util_orders_delivery_metrics') }} U
    ON O.order_id = U.order_id