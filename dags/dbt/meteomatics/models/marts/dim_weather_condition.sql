{{ config(materialized='table') }}

select distinct
  {{ dbt_utils.generate_surrogate_key(['parameter']) }} as condition_key,
  parameter as parameter_code,
  split_part(parameter, ':', 1) as parameter_name,
  split_part(parameter, ':', 2) as unit
from {{ ref('stg_weather_raw') }}
