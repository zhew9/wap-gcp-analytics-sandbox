{{ config(alias='fct_transactions') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_fct_transactions as  (
    select 
        *
    from {{ ref('fct_transactions') }}
)

select * from live_fct_transactions