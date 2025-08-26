/*  
    This is only SCD1 tables to be made incremental, as it's the one expected to grow the
    the fastest, you have may not want to move from 'table'/full-refresh to 'incremental' materialization
    until your table grows large enough to warrant the extra logic for performance.
*/

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        partition_by={
            "field": "registered_at",
            "data_type": "timestamp",
            "granularity": "month"
        },
        unique_key="customer_id",
        cluster_by=["province", "signup_platform"],
        on_schema_change='fail'
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with updated_customers_history as (
    select
    *
    from {{ ref('dim_customers_history') }}
    where is_current_record = true
    {% if is_incremental() %}
        -- Scenario A: A variable-driven run, the default for our Dagster-partitioned dbt assets
        {% if var('start_date', false) and var('end_date', false) %}
        -- The upper bound is strict. We never process beyond the specified end date.
        and date(updated_at) < date('{{ var("end_date") }}')
        -- We look back 3 days (can customize to w/e lateness buffer makes sense for you) from the 
        -- start of the specified window to catch any late-arriving data from the previous period.
        and date(updated_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        -- Scenario B: A classic incremental run (no variables provided)
        {% else %}
        -- This is the fallback for runs without variables. It uses the
        -- max timestamp from the target table as its reference.
        date(updated_at) >= (
            select
                date_sub(max(updated_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
)

select * from updated_customers_history