-- models/marts/bridge_product_suppliers_history.sql

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        partition_by={
            "field": "relationship_started_at",
            "data_type": "timestamp",
            "granularity": "month"
        },
        cluster_by=["supplier_sk","product_sk"],
        unique_key='product_supplier_sk',
        on_schema_change='fail',
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with source_bridge as (
    -- This is your raw source_bridge table from the source system
    select
        product_id,
        supplier_id,
        added_at,
        modified_at,
        removed_at,
        removal_reason
    from {{ ref('stg_retail__products_suppliers_bridge') }}
    {% if is_incremental() %}
    where
        -- Scenario A: A variable-driven run, the default for our Dagster-partitioned dbt assets
        {% if var('start_date', false) and var('end_date', false) %}
        -- The upper bound is strict. We never process beyond the specified end date.
        date(modified_at) < date('{{ var("end_date") }}')
        -- We look back 3 days (can customize to w/e lateness buffer makes sense for you) from the 
        -- start of the specified window to catch any late-arriving data from the previous period.
        and date(modified_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        -- Scenario B: A classic incremental run (no variables provided)
        {% else %}
        -- This is the fallback for runs without variables. It uses the
        -- max timestamp from the target table as its reference.
        date(modified_at) >= (
            select
                date_sub(max(relationship_modified_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),

products_history as (
    -- Your SCD2 dimension for products
    select
        product_sk,
        product_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_products_history') }}
),

suppliers_history as (
    -- Your SCD2 dimension for suppliers
    select
        supplier_sk,
        supplier_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_suppliers_history') }}
),

product_and_supplier_versions_joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['products_history.product_sk', ' suppliers_history.supplier_sk']) }} as product_supplier_sk,
        -- The version-specific surrogate keys
        products_history.product_sk,
        suppliers_history.supplier_sk,

        -- Timestamps for the relationship itself
        source_bridge.added_at as relationship_started_at,
        source_bridge.modified_at as relationship_modified_at,
        source_bridge.removed_at as relationship_ended_at,
        source_bridge.removal_reason as relationship_end_reason
    from source_bridge
    -- Join to find the correct PRODUCT version
    -- The key is to join where the relationship's start date falls within the product version's valid window
    left join products_history
        on source_bridge.product_id = products_history.product_id
        and source_bridge.added_at >= products_history.valid_from_ts
        and source_bridge.modified_at < coalesce(products_history.valid_to_ts, '9999-12-31')
    -- Join to find the correct SUPPLIER version
    -- using the same temporal logic
    left join suppliers_history
        on source_bridge.supplier_id = suppliers_history.supplier_id
        and source_bridge.added_at >= suppliers_history.valid_from_ts
        and source_bridge.modified_at < coalesce(suppliers_history.valid_to_ts, '9999-12-31')
)

select * from product_and_supplier_versions_joined