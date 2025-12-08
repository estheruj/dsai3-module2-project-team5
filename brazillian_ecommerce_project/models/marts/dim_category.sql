SELECT DISTINCT
  product_category_name,
  product_category_name_english 
FROM {{ ref('stg_category_translation') }} 