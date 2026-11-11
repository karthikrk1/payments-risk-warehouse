-- One row per calendar day from dim_date_start to dim_date_end (exclusive); see dbt_project.yml.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="cast('" ~ var('dim_date_end') ~ "' as date)"
    ) }}

),

days as (

    select cast(date_day as date) as date_day from spine

)

select
    date_day,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day_of_month_number,
    -- ISO day of week, 1 = Monday ... 7 = Sunday. DuckDB: isodow; Snowflake: dayofweekiso.
    extract(isodow from date_day) as iso_day_of_week_number,
    extract(isodow from date_day) in (6, 7) as is_weekend,
    -- Week starts Monday in DuckDB; on Snowflake this follows the WEEK_START parameter.
    cast({{ dbt.date_trunc('week', 'date_day') }} as date) as week_start_date,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start_date
from days
