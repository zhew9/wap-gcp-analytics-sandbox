{{ config(alias='dim_customers_history') }}

-- {{ source('write_audit_publish', 'audit_dataset_validated') }}

with live_dim_customers_history as  (
    select 
        *
    from {{ ref('dim_customers_history') }}
)

select * from live_dim_customers_history