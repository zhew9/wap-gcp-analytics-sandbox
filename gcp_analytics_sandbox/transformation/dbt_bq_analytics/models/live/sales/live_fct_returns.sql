{{ config(alias='fct_returns') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_fct_returns as  (
    select 
        *
    from {{ ref('fct_returns') }}
)

select * from live_fct_returns