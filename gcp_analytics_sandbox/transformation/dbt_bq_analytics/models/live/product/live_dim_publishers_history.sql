{{ config(alias='dim_publishers_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_publishers_history as  (
    select 
        *
    from {{ ref('dim_publishers_history') }}
)

select * from live_dim_publishers_history