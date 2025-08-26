{{ config(alias='dim_categories_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_categories_history as  (
    select 
        *
    from {{ ref('dim_categories_history') }}
)

select * from live_dim_categories_history