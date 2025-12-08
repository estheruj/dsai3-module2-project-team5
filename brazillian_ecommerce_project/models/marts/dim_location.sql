SELECT 
  {{ dbt_utils.generate_surrogate_key(['zip_prefix']) }} AS location_key,
  zip_prefix, 
  city, 
  state, 
  latitude, 
  longitude 
FROM {{ ref('util_geo_zip_centroid') }}
  