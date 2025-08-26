{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        partition_by={
            "field": "valid_from_ts",
            "data_type": "timestamp",
            "granularity": "month"
        },
        cluster_by=["updated_at","promotion_id"],
        unique_key='promotion_sk',
        on_schema_change='fail',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with updated_promotion_snapshots as (
    select 
        * 
    from {{ ref('stg_snapshot_retail__promotions') }}
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

final as (
    select
        -- Surrogate Key for the dimension
        dbt_scd_id as promotion_sk,
        -- The original business key 
        promotion_id,
        promotion_uuid,
        -- Promotion details
        promotion_name,
        promotion_code,
        promotion_description,
        discount_level,
        discount_pool,
        stackable,
        priority,
        discount_type,
        discount_value,
        min_quantity,
        min_subtotal,
        promotion_group_id,
        promotion_status,
        promotion_starts_at,
        promotion_ends_at,
        added_at,
        removed_at,
        modified_at,
        -- Snapshot metadata for SCD Type 2
        dbt_valid_from as valid_from_ts,
        dbt_valid_to as valid_to_ts,
        dbt_updated_at as updated_at,
        is_current_record
    from updated_promotion_snapshots
)

select * from final