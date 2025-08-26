-- models/marts/dim_authors.sql

{{ 
    config(
        materialized='table',
        unique_key='author_id'
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_authors as (
    select
        *
    from {{ ref('dim_authors_history') }}
    where is_current_record = true
)

select * from current_authors