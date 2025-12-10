SELECT 
  review_id, 
  order_id, 
  review_score, 
  review_comment_title, 
  review_comment_message, 
  review_creation_date AS review_creation_ts, 
  review_answer_timestamp  AS review_answer_ts,
  CASE WHEN review_score <=2 THEN 
    TRUE
  ELSE 
    FALSE
  END
  AS low_score_flag,
  CASE WHEN LENGTH(review_comment_message) > 0 THEN
    TRUE
  ELSE 
    FALSE
  END
  AS complaint_text_present
FROM {{ ref('stg_order_reviews') }} 
