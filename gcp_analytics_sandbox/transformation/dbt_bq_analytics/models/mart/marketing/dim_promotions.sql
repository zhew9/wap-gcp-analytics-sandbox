-- models/marts/dim_authors.sql

{{ 
    config(
        materialized='table',
        unique_key='promotion_id',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_promotions as (
    select
        *
    from {{ ref('dim_promotions_history') }}
    where is_current_record = true
)

select * from current_promotions