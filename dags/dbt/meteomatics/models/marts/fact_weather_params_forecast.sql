{{ config(materialized='incremental') }}




with recent as (

    select *
    from {{ ref('stg_weather_raw') }}
    where reading_datetime between dateadd('day', -2, current_date())
                              and dateadd('day', 7, current_date())

),

ranked as (

    select *,
           row_number() over (
               partition by city, parameter, reading_datetime
               order by dateGenerated desc
           ) as row_num
    from recent

), 



 latest as (
    select * from ranked
    where parameter not in ('sunrise:sql', 'sunset:sql')
    and reading_datetime > dateGenerated
    and row_num = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['country', 'city']) }} as location_key,
        {{ dbt_utils.generate_surrogate_key(['parameter']) }} as condition_key,
        to_char(reading_datetime, 'YYYYMMDD')::int as date_key,
        reading_datetime,
        reading_value::FLOAT as reading_value
    from latest
)

select * from final
