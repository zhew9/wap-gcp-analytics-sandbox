-- models/staging_snapshots/stg_snapshot_retail__products.sql

with source as (
    select * from {{ ref('snapshot_retail__products') }}
),

renamed as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id,
        -- The original business key
        product_id,
        product_uuid,
        -- Product details
        name as product_name,
        description as product_description,
        year_published,
        cost_price,
        msrp,
        unit_price,
        status as product_status,
        date_added as added_at,
        date_removed as removed_at,
        date_modified as modified_at,
        -- Snapshot metadata
        dbt_valid_from,
        dbt_valid_to,
        (dbt_valid_to is null) as is_current_record,
        dbt_updated_at
    from source
    where date_deleted is null
)

select * from renamed