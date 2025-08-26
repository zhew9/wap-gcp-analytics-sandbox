-- models/marts/dim_channels.sql

{{ 
    config(
        materialized='table',
        unique_key='channel_id',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_channels as (
    select
        *
    from {{ ref('dim_channels_history') }}
    where is_current_record = true
)

select * from current_channels