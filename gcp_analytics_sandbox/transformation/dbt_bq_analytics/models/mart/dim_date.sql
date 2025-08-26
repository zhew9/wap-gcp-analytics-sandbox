-- models/marts/dim_date.sql

{{
    config(
        materialized='table',
        unique_key='date_key',
        on_schema_change='fail'
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with date_dimensions as (
    select 
        *
    from {{ ref('stg_retail__date_dimensions') }}
),

final as (
    select
        date_key,
        full_date,
        year_number,
        month_number,
        day_of_month,
        day_of_week_number,
        day_of_week_name,
        day_of_week_short_name,
        day_of_year,
        iso_week_number,
        iso_year,
        us_week_of_year,
        month_name,
        month_short_name,
        year_month_number,
        year_month_name,
        first_day_of_month,
        last_day_of_month,
        days_in_month,
        quarter_number,
        quarter_name,
        year_quarter_number,
        year_quarter_name,
        first_day_of_quarter,
        last_day_of_quarter,
        first_day_of_year,
        last_day_of_year,
        is_leap_year,
        is_weekday,
        is_weekend,
        is_holiday,
        holiday_name,
        is_workday,
        day_suffix,
        week_ending_date_saturday,
        week_ending_date_sunday,
        fiscal_year_number,
        fiscal_quarter_number,
        fiscal_month_offset
    from date_dimensions
)

select * from final
