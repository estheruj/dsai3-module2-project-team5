SELECT
    product_id, 
    LOWER(product_category_name) AS product_category_name, 
    LOWER(product_category_name_english) AS product_category_name_english
FROM {{ source('brazilian_ecommerce', 'product_category_name_translation') }}