{{ config(alias='dim_promotions_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_promotions_history as  (
    select 
        *
    from {{ ref('dim_promotions_history') }}
)

select * from live_dim_promotions_history