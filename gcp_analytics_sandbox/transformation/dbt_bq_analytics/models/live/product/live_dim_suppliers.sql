{{ config(alias='dim_suppliers') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_suppliers as  (
    select 
        *
    from {{ ref('dim_suppliers') }}
)

select * from live_dim_suppliers