-- models/marts/fct_sales.sql

-- clustered by product_key first, assuming most analysis will be product based over customer based
{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        unique_key='transaction_line_id',
        partition_by={
            "field": "sale_event_ts",
            "data_type": "timestamp",
            "granularity": "day"
            },
        cluster_by=["product_sk", "customer_sk"],
        on_schema_change='fail'
    )
}}


-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with purchase_line_proration as (
    -- intermediate model does all the heavy lifting for calculating purchase_line_proration of tax and header level discounts.
    select
        transaction_line_id,
        -- a chunk of degenerate dimensions because we are not building a transaction header dimension table
        -- so if we want to add these to fct_returns or fct_sales, better to include them as 
        -- degenerate dimensions instead of joining 2 potential fact tables (our headers will be another fact table)
        transaction_id,
        line_number,
        transaction_uuid,
        customer_id,
        channel_id,
        product_id,
        quantity,
        gross_line_total,
        line_discount_amount,
        prorated_discount_amount,
        total_discount_amount,
        pretax_net_amount,
        prorated_tax_amount,
        net_amount,
        line_created_at,
        modified_at
    from {{ ref('int_retail__transaction_lines_proration') }}
    where
        transaction_type = 'Purchase'
    {% if is_incremental() %}
    -- Logic to handle targeted backfills or standard incremental runs
        {% if var('start_date', false) and var('end_date', false) %}
        -- If variables are passed, run for a specific date range (for backfills)
        and date(modified_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        and date(modified_at) < date('{{ var("end_date") }}')
        {% else %}
        -- Otherwise, run for new data since the last run (standard incremental load)
        -- with a 3-day lookback to catch late-arriving data.
        and date(modified_at) >= (
            select
                date_sub(date(max(source_modified_at)), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
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
),

channels_history as (
    select 
        channel_sk, 
        channel_id, 
        valid_from_ts, 
        valid_to_ts
    from {{ ref('dim_channels_history') }}
),


dim_date as (
    select
        date_key,
        full_date
    from {{ ref('dim_date') }}
),

final_sales_table as (
    select
        -- Primary Key for the fact table
        purchase_line_proration.transaction_line_id,

        -- Degenerate Dimensions & other IDs for traceability
        purchase_line_proration.transaction_id,
        purchase_line_proration.line_number,
        purchase_line_proration.transaction_uuid,        

        -- Use coalesce to handle potential nulls, assigning to a 'Unknown' key,
        -- the type for the second argument you pass to coalesce() depends on how you generated your
        -- surrogate_keys, and you may need to cast your coalesce arguments
        coalesce(safe_cast(customers_history.customer_sk as string), '-1') as customer_sk,
        coalesce(safe_cast(channels_history.channel_sk as string), '-1') as channel_sk,

        coalesce(safe_cast(products_history.product_sk as string), '-1') as product_sk,
        purchase_line_proration.quantity,

        -- Measures from the intermediate model
        purchase_line_proration.gross_line_total,
        purchase_line_proration.line_discount_amount,
        purchase_line_proration.prorated_discount_amount,
        purchase_line_proration.total_discount_amount,
        purchase_line_proration.pretax_net_amount,
        purchase_line_proration.prorated_tax_amount,
        purchase_line_proration.net_amount,

        coalesce(safe_cast(dim_date.date_key as string), '-1') as sale_date_key,
        -- Timestamps
        purchase_line_proration.line_created_at as sale_event_ts,
        purchase_line_proration.modified_at as source_modified_at,
        
    -- Join to the dimension tables on the original business keys
    from purchase_line_proration
    left join products_history
        on purchase_line_proration.product_id = products_history.product_id -- Use purchase_line_proration.product_id
        and purchase_line_proration.line_created_at >= products_history.valid_from_ts
        and purchase_line_proration.line_created_at < coalesce(products_history.valid_to_ts, timestamp('9999-12-31'))
    left join customers_history
        on purchase_line_proration.customer_id = customers_history.customer_id -- Use purchase_line_proration.customer_id
        and purchase_line_proration.line_created_at >= customers_history.valid_from_ts
        and purchase_line_proration.line_created_at < coalesce(customers_history.valid_to_ts, timestamp('9999-12-31'))
    left join channels_history
        on purchase_line_proration.channel_id = channels_history.channel_id
        and purchase_line_proration.line_created_at >= channels_history.valid_from_ts
        and purchase_line_proration.line_created_at < coalesce(channels_history.valid_to_ts, timestamp('9999-12-31'))
    left join dim_date 
        on cast(purchase_line_proration.line_created_at as date) = dim_date.full_date
)

select * from final_sales_table