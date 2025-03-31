{{ config(materialized='table') }}

with calendar as (
    select
        dateadd(day, seq4(), to_date('2020-01-01')) as date_day
    from table(generator(rowcount => 3653))  -- 10 years
),

final as (
    select
        calendar.date_day,
        to_char(calendar.date_day, 'YYYYMMDD')::int as date_key,
        extract(year from calendar.date_day) as year,
        extract(month from calendar.date_day) as month,
        extract(day from calendar.date_day) as day,
        to_char(calendar.date_day, 'Day') as weekday,
        case
            when extract(dow from calendar.date_day) in (6, 0) then true
            else false
        end as is_weekend,
        -- 📅 Week number since Jan 1st (starting on Monday)
        datediff(
            week,
            date_trunc('week', to_date(year || '-01-01')),  -- Monday of first week of the year
            date_trunc('week', calendar.date_day)
        ) + 1 as week_number
    from calendar
)

select * from final