{{ config(alias='dim_suppliers_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_suppliers_history as  (
    select 
        *
    from {{ ref('dim_suppliers_history') }}
)

select * from live_dim_suppliers_history