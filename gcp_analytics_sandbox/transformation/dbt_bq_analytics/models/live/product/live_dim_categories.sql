{{ config(alias='dim_categories') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_categories as  (
    select 
        *
    from {{ ref('dim_categories') }}
)

select * from live_dim_categories