{{ config(alias='dim_channels_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_channels_history as  (
    select 
        *
    from {{ ref('dim_channels_history') }}
)

select * from live_dim_channels_history