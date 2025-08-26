-- models/staging/snapshots/stg_snapshot_retail__promotions.sql

with source as (
    select * from {{ ref('snapshot_retail__promotions') }}
),

renamed as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id,
        -- The original business key 
        promotion_id,
        promotion_uuid,
        -- Promotion details
        promotion_name,
        promotion_code,
        description as promotion_description,
        discount_level,
        discount_pool,
        stackable,
        priority,
        discount_type,
        discount_value,
        min_quantity,
        min_subtotal,
        promotion_group_id,
        status as promotion_status,
        start_date as promotion_starts_at,
        end_date as promotion_ends_at,
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