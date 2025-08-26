{{ config(alias='brg_products_suppliers_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_brg_products_suppliers_history as  (
    select 
        *
    from {{ ref('brg_products_suppliers_history') }}
)

select * from live_brg_products_suppliers_history