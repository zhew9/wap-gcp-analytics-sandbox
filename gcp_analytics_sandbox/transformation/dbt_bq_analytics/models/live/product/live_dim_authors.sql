{{ config(alias='dim_authors') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_authors as  (
    select 
        *
    from {{ ref('dim_authors') }}
)

select * from live_dim_authors