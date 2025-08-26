{{ config(alias='dim_promotions') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_promotions as  (
    select 
        *
    from {{ ref('dim_promotions') }}
)

select * from live_dim_promotions