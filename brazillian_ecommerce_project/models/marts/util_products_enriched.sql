SELECT 
    P.product_id, 
    T.product_category_name, 
    T.product_category_name_english, 
    P.product_name_length, 
    P.product_description_length, 
    P.product_photos_qty, 
    P.product_weight_g, 
    P.product_length_cm, 
    P.product_height_cm, 
    P.product_width_cm
FROM {{ ref('stg_products') }} P
LEFT JOIN {{ ref('stg_category_translation') }} T
  ON P.product_category_name = T.product_category_name