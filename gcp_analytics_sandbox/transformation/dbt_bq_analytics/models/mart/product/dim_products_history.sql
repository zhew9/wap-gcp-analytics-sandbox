-- models/marts/product/dim_products_history.sql

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        partition_by={
            "field": "valid_from_ts",
            "data_type": "timestamp",
            "granularity": "month"
        },
        unique_key='product_sk',
        on_schema_change='fail',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with updated_product_snapshots as (
    select
        *
    from {{ ref('stg_snapshot_retail__products') }}
    -- if the model is incremental, limit query to only newly updated records to avoid full scans
    -- using a filter for newly updated records since the last run, with a 3-day lateness buffer for safety
    {% if is_incremental() %}
    where
        -- Scenario A: A variable-driven run, the default for our Dagster-partitioned dbt assets
        {% if var('start_date', false) and var('end_date', false) %}
        -- The upper bound is strict. We never process beyond the specified end date.
        date(dbt_updated_at) < date('{{ var("end_date") }}')
        -- We look back 3 days (can customize to w/e lateness buffer makes sense for you) from the 
        -- start of the specified window to catch any late-arriving data from the previous period.
        and date(dbt_updated_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        -- Scenario B: A classic incremental run (no variables provided)
        {% else %}
        -- This is the fallback for runs without variables. It uses the
        -- max timestamp from the target table as its reference.
        date(dbt_updated_at) >= (
            select
                date_sub(max(updated_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),

-- note: if you expect or need to allow for extremely late data, and also if your table
-- isn't very large, it's better to just do a full refresh and you won't have to worry about lateness

-- you can also do delete + insert if your want more granularity and your warehouse supports it 

final as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id as product_sk,
        -- The original business key
        product_id,
        product_uuid,
        -- Product details
        product_name,
        product_description,
        year_published,
        cost_price,
        msrp,
        unit_price,
        product_status,
        -- Snapshot metadata
        dbt_valid_from as valid_from_ts,
        dbt_valid_to as valid_to_ts,
        dbt_updated_at as updated_at,
        is_current_record
    from updated_product_snapshots
)

select * from final