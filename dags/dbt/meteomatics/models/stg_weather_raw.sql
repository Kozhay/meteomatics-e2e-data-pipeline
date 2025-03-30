-- models/stg_weather_raw.sql

{{ config(
    materialized='incremental',
    unique_key='file_path || parameter || reading_datetime',
    cluster_by=['reading_datetime::DATE']
) }}



WITH first_level AS (
  SELECT 
    METADATA$FILENAME AS file_path,
    METADATA$FILE_LAST_MODIFIED AS file_modified,
    $1:country::STRING AS country,
    $1:city::STRING AS city,
    $1:weather.dateGenerated::TIMESTAMP AS dateGenerated,
    $1:weather.data AS weather_data
  FROM @{{source('s3_stage','my_s3_stage')}}

{% if is_incremental() %}
WHERE METADATA$FILENAME NOT IN (
  SELECT file_path FROM {{ this }}
)
{% endif %}
)

SELECT 
  fl.file_path,
  fl.file_modified,
  fl.country,
  fl.city,
  fl.dateGenerated,
  param.value:parameter::STRING AS parameter,
  coord.value:lat::FLOAT AS latitude,
  coord.value:lon::FLOAT AS longitude,
  reading.value:date::TIMESTAMP AS reading_datetime,
  reading.value:value AS reading_value
FROM first_level fl
  , LATERAL FLATTEN(INPUT => fl.weather_data) AS param
  , LATERAL FLATTEN(INPUT => param.value:coordinates) AS coord
  , LATERAL FLATTEN(INPUT => coord.value:dates) AS reading
