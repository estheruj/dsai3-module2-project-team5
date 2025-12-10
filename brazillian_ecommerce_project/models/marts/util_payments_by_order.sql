WITH payment_frequency AS (
    -- CTE 1: Calculate the frequency of each payment_type per order.
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        -- Calculate the total count (frequency) for each unique payment_type within an order
        COUNT(payment_type) OVER (
            PARTITION BY order_id, payment_type
        ) AS payment_type_count,
        -- Calculate the overall aggregates (these are safe and stable)
        SUM(payment_value) OVER (PARTITION BY order_id) AS total_payment_value,
        MAX(payment_installments) OVER (PARTITION BY order_id) AS max_installments
    FROM
        {{ ref('stg_order_payments') }}
),

payment_ranking AS (
    -- CTE 2: Determine the rank for the Primary Payment Type (Mode/First).
    -- Now we can use the pre-calculated 'payment_type_count' in the ORDER BY.
    SELECT
        order_id,
        payment_type,
        total_payment_value,
        max_installments,
        -- Assign a rank: 1. By frequency (Mode), 2. By sequential order (First)
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                payment_type_count DESC, -- Primary sort: Most frequent first (Mode)
                payment_sequential ASC   -- Secondary sort: First in sequence (Tie-breaker)
        ) AS mode_rank
    FROM
        payment_frequency
)

-- Final SELECT: Aggregate to the final grain (1 row per order_id).
SELECT
    order_id,
    MAX(total_payment_value) AS total_payment_value, -- MAX is safe as values are identical per order
    MAX(max_installments) AS max_installments,       -- MAX is safe as values are identical per order
    -- Select the payment_type associated with the highest rank (Mode/First)
    MAX(
        CASE
            WHEN mode_rank = 1 THEN payment_type
            ELSE NULL
        END
    ) AS primary_payment_type
FROM
    payment_ranking
GROUP BY
    order_id