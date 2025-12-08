{% set min_date_query %}
    (SELECT MIN(DATE(order_purchase_timestamp)) FROM {{ ref('stg_orders') }})
{% endset %}

{% set max_date_query %}
    (SELECT DATE_ADD(MAX(DATE(order_purchase_timestamp)), INTERVAL 30 DAY) FROM {{ ref('stg_orders') }})
{% endset %}

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(ISOWEEK FROM date_day) AS week_of_year,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) = 1
        OR EXTRACT(DAYOFWEEK FROM date_day) = 7
        THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM UNNEST(
    GENERATE_DATE_ARRAY(
        {{ min_date_query }},
        {{ max_date_query }},
        INTERVAL 1 DAY
    )
) AS date_day