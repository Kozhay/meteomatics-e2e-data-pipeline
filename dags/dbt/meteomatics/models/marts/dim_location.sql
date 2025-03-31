{{ config(materialized='table') }}

select distinct
  {{ dbt_utils.generate_surrogate_key(['country', 'city']) }} as location_key,
  country,
  city,
  latitude,
  longitude
from {{ ref('stg_weather_raw') }}