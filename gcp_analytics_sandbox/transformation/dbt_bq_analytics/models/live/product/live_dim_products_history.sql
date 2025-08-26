{{ config(alias='dim_products_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_products_history as  (
    select 
        *
    from {{ ref('dim_products_history') }}
)

select * from live_dim_products_history