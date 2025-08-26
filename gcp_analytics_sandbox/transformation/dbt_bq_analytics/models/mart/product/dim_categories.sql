-- models/marts/dim_categories.sql

{{ 
    config(
        materialized='table',
        unique_key='category_id',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_categories as (
    select
        *
    from {{ ref('dim_categories_history') }}
    where is_current_record = true
)

select * from current_categories