SELECT 
  product_id,
  product_category_name AS category_name_pt, 
  product_name_length, 
  product_description_length, 
  product_photos_qty, 
  product_weight_g, 
  product_length_cm, 
  product_height_cm, 
  product_width_cm 
FROM {{ ref('util_products_enriched') }} 