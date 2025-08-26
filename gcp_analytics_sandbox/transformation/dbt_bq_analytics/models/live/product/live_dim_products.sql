{{ config(alias='dim_products') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_products as  (
    select 
        *
    from {{ ref('dim_products') }}
)

select * from live_dim_products