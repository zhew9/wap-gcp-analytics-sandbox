{{ config(alias='fct_payments') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_fct_payments as  (
    select 
        *
    from {{ ref('fct_payments') }}
)

select * from live_fct_payments