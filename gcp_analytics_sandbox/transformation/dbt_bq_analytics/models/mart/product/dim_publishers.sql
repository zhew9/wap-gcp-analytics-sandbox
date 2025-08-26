-- models/marts/dim_publishers.sql

{{ 
    config(
        materialized='table',
        unique_key='supplier_id',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_publishers as (
    select
        *
    from {{ ref('dim_publishers_history') }}
    where is_current_record = true
)

select * from current_publishers