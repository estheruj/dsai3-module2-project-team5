with reviews as (
    select *
    from {{ ref('stg_order_reviews') }}
),

orders as (
    select
        order_id,
        customer_id
    from {{ ref('stg_orders') }}
)

select
    r.review_id,

    -- FKs
    o.customer_id,
    r.order_id,
    cast(format_date('%Y%m%d', date(r.review_creation_date)) as int64)
        as review_creation_date_key,
    cast(format_date('%Y%m%d', date(r.review_answer_timestamp)) as int64)
        as review_answer_date_key,

    -- Attributes
    r.review_comment_title,
    r.review_comment_message,

    -- Measures
    r.review_score

from reviews r
left join orders o
    on r.order_id = o.order_id
