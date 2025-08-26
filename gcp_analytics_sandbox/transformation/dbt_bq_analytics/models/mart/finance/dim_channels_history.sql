-- models/marts/sales/dim_customer_history.sql

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        partition_by={
            "field": "valid_from_ts",
            "data_type": "timestamp",
            "granularity": "month"
        },
        unique_key='channel_sk',
        on_schema_change='fail',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with updated_channels_snapshot as (
    select
        *
    from {{ ref('stg_snapshot_retail__channels') }}
    -- if the model is incremental, limit query to only newly updated records to avoid full scans
    -- with a lag of 3 days to account for potentially late data
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
        -- Surrogate Key for the dimension
        dbt_scd_id as channel_sk,
        -- The original business key
        channel_id,
        -- Channel details
        channel_name,
        added_at,
        removed_at,
        modified_at as source_modified_at,
        -- Snapshot metadata for SCD Type 2
        dbt_valid_from as valid_from_ts,
        dbt_valid_to as valid_to_ts,
        dbt_updated_at as updated_at,
        is_current_record
    from updated_channels_snapshot
)

select * from final