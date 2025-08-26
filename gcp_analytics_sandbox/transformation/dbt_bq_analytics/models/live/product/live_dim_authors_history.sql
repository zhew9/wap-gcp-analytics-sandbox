{{ config(alias='dim_authors_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_authors_history as  (
    select 
        *
    from {{ ref('dim_authors_history') }}
)

select * from live_dim_authors_history