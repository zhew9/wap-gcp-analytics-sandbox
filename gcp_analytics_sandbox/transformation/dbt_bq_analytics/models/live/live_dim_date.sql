{{ config(alias='dim_date') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_date as  (
    select 
        *
    from {{ ref('dim_date') }}
)

select * from live_dim_date