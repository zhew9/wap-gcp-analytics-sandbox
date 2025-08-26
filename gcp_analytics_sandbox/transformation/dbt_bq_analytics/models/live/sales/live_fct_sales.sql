{{ config(alias='fct_sales') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_fct_sales as  (
    select 
        *
    from {{ ref('fct_sales') }}
)

select * from live_fct_sales