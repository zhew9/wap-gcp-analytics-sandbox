-- models/staging_snapshots/stg_snapshot_retail__suppliers.sql

with source as (
    select * from {{ ref('snapshot_retail__suppliers') }}
),

renamed as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id,
        -- The original business key
        supplier_id,
        supplier_uuid,
        -- Supplier details
        supplier_name,
        description as supplier_description,
        country,
        city,
        province,
        address as street_address,
        postal_code,
        contact_name,
        contact_number,
        contact_email,
        date_added as added_at,
        date_removed as removed_at,
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