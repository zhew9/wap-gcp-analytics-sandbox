{{ config(alias='brg_products_publishers_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_brg_products_publishers_history as  (
    select 
        *
    from {{ ref('brg_products_publishers_history') }}
)

select * from live_brg_products_publishers_history