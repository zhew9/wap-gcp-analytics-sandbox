{{ config(alias='dim_promotion_groups_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_promotion_groups_history as  (
    select 
        *
    from {{ ref('dim_promotion_groups_history') }}
)

select * from live_dim_promotion_groups_history