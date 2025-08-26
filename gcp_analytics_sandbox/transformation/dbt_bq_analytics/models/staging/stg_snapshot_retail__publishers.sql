-- models/staging/snapshots/stg_snapshot_retail__publishers.sql

with source as (
    select * from {{ ref('snapshot_retail__publishers') }}
),

renamed as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id,
        -- The original business key
        publisher_id,
        publisher_uuid,
        -- Publisher details
        publisher_name,
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