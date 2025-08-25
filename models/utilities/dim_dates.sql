{{ config(materialized="view") }}

with
    -- build date rows from 2023-07-01 to 2025-12-31
    date_spine as ({{ dbt_date.get_date_dimension("2023-07-01", "2025-12-31") }}),

    date_formatting as (
        select
            date_day,

            -- day/weekday/iso-week
            {{ dbt_date.day_of_month("date_day") }} as day_of_month,
            {{ dbt_date.day_name("date_day", short=true) }} as day_of_week,
            {{ dbt_date.iso_week_of_year("date_day") }} as iso_week,
            {{ dbt_date.day_of_week("date_day") }} as iso_weekday,

            -- month/quarter/year
            {{ dbt_date.month_name("date_day", short=true) }} as month_name,
            {{ dbt_date.date_part("quarter", "date_day") }} as quarter,
            {{ dbt_date.date_part("year", "date_day") }} as year,

            -- handy label (DuckDB/MotherDuck)
            strftime('%Y-%m', date_day) as year_month,

            -- load stamp (respects dbt_date time zone variable)
            {{ dbt_date.now() }} as loaded_at
        from date_spine
    )

select *
from date_formatting
