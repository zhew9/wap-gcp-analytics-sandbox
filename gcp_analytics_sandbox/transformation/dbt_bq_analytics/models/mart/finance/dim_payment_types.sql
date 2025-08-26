-- models/marts/dim_payment_types.sql

{{ 
    config(
        materialized='table',
        unique_key='payment_type_id',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_payment_types as (
    select
        *
    from {{ ref('dim_payment_types_history') }}
    where is_current_record = true
)

select * from current_payment_types