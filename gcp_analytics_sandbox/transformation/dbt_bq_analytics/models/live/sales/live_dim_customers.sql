{{ config(alias='dim_customers') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_customers as  (
    select 
        *
    from {{ ref('dim_customers') }}
)

select * from live_dim_customers