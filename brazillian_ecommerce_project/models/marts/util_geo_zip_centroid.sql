WITH
  zip_counts AS (
  -- 1. Count the occurrences of each city and state per zip code prefix
  SELECT
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state,
    geolocation_lat,
    geolocation_lng,
    COUNT(*) OVER (PARTITION BY geolocation_zip_code_prefix, geolocation_city) AS city_count,
    COUNT(*) OVER (PARTITION BY geolocation_zip_code_prefix, geolocation_state) AS state_count
  FROM
    {{ ref('stg_geolocation') }}
),
  ranked_locations AS (
  -- 2. Rank the city and state by frequency for each zip code prefix
  SELECT
    *,
    -- Rank cities by frequency (highest count first), then by city name (stable fallback)
    ROW_NUMBER() OVER (
        PARTITION BY geolocation_zip_code_prefix
        ORDER BY city_count DESC, geolocation_city ASC
    ) AS city_rank,
    -- Rank states by frequency (highest count first), then by state name (stable fallback)
    ROW_NUMBER() OVER (
        PARTITION BY geolocation_zip_code_prefix
        ORDER BY state_count DESC, geolocation_state ASC
    ) AS state_rank
  FROM
    zip_counts
),
  selected_location AS (
  -- 3. Select the single most frequent city and state for each zip code prefix
  SELECT
    geolocation_zip_code_prefix AS zip_prefix,
    MAX(CASE WHEN city_rank = 1 THEN geolocation_city END) AS selected_city,
    MAX(CASE WHEN state_rank = 1 THEN geolocation_state END) AS selected_state,
    geolocation_lat,
    geolocation_lng
  FROM
    ranked_locations
  GROUP BY
    geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
)
-- 4. Final aggregation to get the required outputs, now with explicit casting
SELECT
  zip_prefix,
  ANY_VALUE(selected_city) AS city,
  ANY_VALUE(selected_state) AS state,
  AVG(CAST(geolocation_lat AS FLOAT64)) AS latitude,
  AVG(CAST(geolocation_lng AS FLOAT64)) AS longitude
FROM
  selected_location
GROUP BY
  zip_prefix
ORDER BY
  zip_prefix