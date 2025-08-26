-- models/staging/snapshots/stg_snapshot_retail__customers.sql

with source as (
    select * from {{ ref('snapshot_retail__customers') }}
),

renamed as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id,
        -- The original business key
        customer_id,
        customer_uuid,
        -- Customer details
        first_name,
        last_name,
        email,
        gender,
        province,
        date_of_birth,
        status as customer_status,
        signup_platform,
        registration_date as registered_at,
        date_modified as modified_at,
        -- Snapshot metadata for SCD Type 2
        dbt_valid_from,
        dbt_valid_to,
        (dbt_valid_to is null) as is_current_record,
        dbt_updated_at
    from source
    where date_deleted is null
)

select * from renamed