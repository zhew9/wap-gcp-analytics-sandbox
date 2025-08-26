-- models/marts/dim_suppliers.sql

{{ 
    config(
        materialized='table',
        unique_key='supplier_id',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_suppliers as (
    select
        *
    from {{ ref('dim_suppliers_history') }}
    where is_current_record = true
)

select * from current_suppliers