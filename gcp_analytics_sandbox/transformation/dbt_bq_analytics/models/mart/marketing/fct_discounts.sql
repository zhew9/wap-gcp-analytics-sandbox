-- models/marts/marketing/fct_discounts.sql

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        unique_key='transaction_discount_id',
        partition_by={
            "field": "discount_event_ts",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["product_sk", "customer_sk", "promotion_sk"],
        on_schema_change='fail'
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with transaction_discounts as (
    -- 1. Select the incremental batch of discounts using the backfill/standard run pattern
    select
        *
    from {{ ref('stg_retail__transaction_discounts') }}
    {% if is_incremental() %}
    -- Logic to handle targeted backfills or standard incremental runs
    where
        {% if var('start_date', false) and var('end_date', false) %}
        -- If variables are passed, run for a specific date range (for backfills)
        date(modified_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        and date(modified_at) < date('{{ var("end_date") }}')
        {% else %}
        -- Otherwise, run for new data since the last run (standard incremental load)
        -- with a 3-day lookback to catch late-arriving data.
        modified_at >= (
            select
                date_sub(max(source_modified_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),

transaction_lines as (
    -- 2. Filter transaction lines to only those relevant to our discount batch
    select
        transaction_line_id,
        transaction_id,
        product_id
    from {{ ref('stg_retail__transaction_lines') }}
    where transaction_line_id in (select transaction_line_id from transaction_discounts)
),

transaction_headers as (
    -- 3. Filter headers to only those relevant to our discount batch
    select
        transaction_id,
        customer_id
    from {{ ref('stg_retail__transaction_headers') }}
    where transaction_id in (select transaction_id from transaction_discounts)
       or transaction_id in (select transaction_id from transaction_lines)
),

discounts_with_keys as (
    -- This CTE brings all the business keys together first
    select
        transaction_discounts.transaction_discount_id,
        transaction_discounts.promotion_id,
        transaction_discounts.transaction_line_id,
        transaction_discounts.transaction_id,
        transaction_discounts.discounted_units,
        transaction_discounts.discount_amount,
        transaction_discounts.discount_sequence,
        transaction_discounts.applied_at,
        transaction_discounts.modified_at,
        transaction_lines.product_id,
        coalesce(th_from_lines.customer_id, th_direct.customer_id) as customer_id
    from transaction_discounts
    left join transaction_lines on transaction_discounts.transaction_line_id = transaction_lines.transaction_line_id
    left join transaction_headers as th_from_lines on transaction_lines.transaction_id = th_from_lines.transaction_id
    left join transaction_headers as th_direct on transaction_discounts.transaction_id = th_direct.transaction_id
),

-- Dimension History Tables (Pre-filtered for performance where possible)
promotions_history as (
    select
        promotion_sk,
        promotion_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_promotions_history') }}
),
products_history as (
    select
        product_sk,
        product_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_products_history') }}
),
customers_history as (
    select
        customer_sk,
        customer_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_customers_history') }}
    where customer_id in (select customer_id from discounts_with_keys)
),
dim_date as (
    select
        date_key,
        full_date
    from {{ ref('dim_date') }}
),

final as (
    select
        -- Primary Key
        discounts_with_keys.transaction_discount_id,

        -- Surrogate Keys from Dimensions
        coalesce(safe_cast(customers_history.customer_sk as string), '-1') as customer_sk,
        coalesce(safe_cast(products_history.product_sk as string), '-1') as product_sk,
        coalesce(safe_cast(promotions_history.promotion_sk as string), '-1') as promotion_sk,
        coalesce(safe_cast(dim_date.date_key as string), '-1') as discount_date_key,

        -- Degenerate Dimensions
        discounts_with_keys.transaction_id,
        discounts_with_keys.transaction_line_id,
        case
            when discounts_with_keys.transaction_id is not null then 'Order'
            when discounts_with_keys.transaction_line_id is not null then 'Line Item'
        end as discount_level,

        -- Measures
        discounts_with_keys.discounted_units,
        discounts_with_keys.discount_amount,
        discounts_with_keys.discount_sequence,

        -- Timestamps
        discounts_with_keys.applied_at as discount_event_ts,
        discounts_with_keys.modified_at as source_modified_at

    from discounts_with_keys
    -- Join to get promotion_sk
    left join promotions_history
        on discounts_with_keys.promotion_id = promotions_history.promotion_id
        and discounts_with_keys.applied_at >= promotions_history.valid_from_ts
        and discounts_with_keys.applied_at < coalesce(promotions_history.valid_to_ts, timestamp('9999-12-31 23:59:59'))
    -- Join to get customer_sk (point-in-time)
    left join customers_history
        on discounts_with_keys.customer_id = customers_history.customer_id
        and discounts_with_keys.applied_at >= customers_history.valid_from_ts
        and discounts_with_keys.applied_at < coalesce(customers_history.valid_to_ts, timestamp('9999-12-31 23:59:59'))
    -- Join to get product_sk (point-in-time)
    left join products_history
        on discounts_with_keys.product_id = products_history.product_id
        and discounts_with_keys.applied_at >= products_history.valid_from_ts
        and discounts_with_keys.applied_at < coalesce(products_history.valid_to_ts, timestamp('9999-12-31 23:59:59'))
    -- Join to get the date_key
    left join dim_date
        on cast(discounts_with_keys.applied_at as date) = dim_date.full_date
)

select * from final