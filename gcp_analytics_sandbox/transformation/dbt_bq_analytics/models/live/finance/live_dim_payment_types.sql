{{ config(alias='dim_payment_types') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_payment_types as  (
    select 
        *
    from {{ ref('dim_payment_types') }}
)

select * from live_dim_payment_types