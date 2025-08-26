{{ config(alias='dim_payment_types_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_payment_types_history as  (
    select 
        *
    from {{ ref('dim_payment_types_history') }}
)

select * from live_dim_payment_types_history