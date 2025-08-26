{{ config(alias='fct_discounts') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_fct_discounts as  (
    select 
        *
    from {{ ref('fct_discounts') }}
)

select * from live_fct_discounts