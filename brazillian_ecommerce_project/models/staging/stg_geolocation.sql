SELECT
    LOWER(geolocation_city) AS geolocation_city,
    geolocation_lat,
    geolocation_lng,
    LOWER(geolocation_state) AS geolocation_state,
    geolocation_zip_code_prefix
FROM {{ source('brazilian_ecommerce', 'geolocation') }}