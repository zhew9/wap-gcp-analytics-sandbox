{{ config(alias='dim_channels') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_channels as  (
    select 
        *
    from {{ ref('dim_channels') }}
)

select * from live_dim_channels