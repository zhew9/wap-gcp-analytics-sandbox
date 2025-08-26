-- models/staging/snapshots/stg_snapshot_retail__authors.sql

with source as (
    select * from {{ ref('snapshot_retail__authors') }}
),

updated as (
    select
        *
    from source
    where
        date(date_modified) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        and date(date_modified) < date('{{ var("end_date") }}')
),

renamed as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id,
        -- The original business key 
        author_id,
        author_uuid,
        -- Author details
        author_name,
        date_added as added_at,
        date_removed as removed_at,
        date_modified as modified_at,
        -- Snapshot metadata for SCD Type 2
        dbt_valid_from,
        dbt_valid_to,
        (dbt_valid_to is null) as is_current_record,
        dbt_updated_at
    from updated
    where date_deleted is null
)

select * from renamed