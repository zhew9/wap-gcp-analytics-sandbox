-- models/marts/dim_products.sql

{{
    config(
        materialized='table',
        unique_key='product_id',
    )
}}

-- just gets the current records/SCD1, faster joins for queries on current data

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with current_products as(
    select
        *
    from {{ ref('dim_products_history') }}
    where is_current_record = true
)

select * from current_products