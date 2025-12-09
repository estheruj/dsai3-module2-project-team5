SELECT Distinct
    S.seller_id,
    L.zip_prefix AS seller_zip_prefix,
    S.seller_city,
    S.seller_state,
    L.location_key
FROM {{ ref('stg_sellers') }} S
LEFT JOIN {{ ref('dim_location') }} L
  ON S.seller_zip_code_prefix = L.zip_prefix
