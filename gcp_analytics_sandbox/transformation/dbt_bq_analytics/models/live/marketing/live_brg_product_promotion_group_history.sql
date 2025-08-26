{{ config(alias='brg_product_promotion_group_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_brg_product_promotion_group_history as  (
    select 
        *
    from {{ ref('brg_product_promotion_group_history') }}
)

select * from live_brg_product_promotion_group_history