{{ config(alias='dim_publishers') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_publishers as  (
    select 
        *
    from {{ ref('dim_publishers') }}
)

select * from live_dim_publishers