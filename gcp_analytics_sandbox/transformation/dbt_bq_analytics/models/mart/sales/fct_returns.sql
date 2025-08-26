-- models/mart/sales/fct_returns.sql

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        partition_by={
            "field": "line_created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["product_sk", "channel_sk", "customer_sk"],
        unique_key='transaction_line_id',
        on_schema_change='fail',
    )
}}


-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with updated_return_lines as (
    select
        transaction_line_id,
        line_number,
        transaction_id,
        transaction_uuid,
        original_transaction_id,

        channel_id,
        customer_id,
        product_id,
        quantity,
        
        -- Final Measures
        gross_line_total,
        line_discount_amount,
        prorated_discount_amount,
        prorated_tax_amount,
        total_discount_amount,
        pretax_net_amount,
        net_amount,
        
        -- Timestamps
        line_created_at,
        modified_at
    from {{ ref('int_retail__transaction_lines_proration') }}
    where
        transaction_type = 'Return'
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
                date_sub(max(source_modified_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),   

-- We need to join to dimension history tables to get the correct point-in-time surrogate keys
dim_customers_history as (
    select  
        customer_sk,
        customer_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_customers_history') }}
),

dim_products_history as (
    select 
        product_sk,
        product_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_products_history') }}
),

dim_channels_history as (
    select 
        channel_sk,
        channel_id,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_channels_history') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

final_returns_table as (
    -- Final SELECT statement
    select
        -- Primary Key for the fact table
        updated_return_lines.transaction_line_id,

        -- Degenerate Dimensions
        updated_return_lines.line_number,
        updated_return_lines.transaction_id,
        updated_return_lines.transaction_uuid,
        updated_return_lines.original_transaction_id,        

        -- Surrogate Keys from Dimensions
        coalesce(safe_cast(dim_customers_history.customer_sk as string), '-1') as customer_sk,
        coalesce(safe_cast(dim_channels_history.channel_sk as string), '-1') as channel_sk,    

        -- Measures (The quantitative facts about the return)
        coalesce(safe_cast(dim_products_history.product_sk as string), '-1') as product_sk,
        updated_return_lines.quantity,

        -- Measures
        updated_return_lines.gross_line_total,
        updated_return_lines.line_discount_amount,
        updated_return_lines.prorated_discount_amount,
        updated_return_lines.total_discount_amount,
        updated_return_lines.pretax_net_amount,
        updated_return_lines.prorated_tax_amount,
        updated_return_lines.net_amount,

        -- Timestamps
        coalesce(safe_cast(dim_date.date_key as string), '-1') as return_date_key,
        updated_return_lines.line_created_at,
        updated_return_lines.modified_at as source_modified_at,

    from updated_return_lines
    -- Point-in-time join for customers based on the return date
    left join dim_customers_history
        on updated_return_lines.customer_id = dim_customers_history.customer_id
        and updated_return_lines.line_created_at >= dim_customers_history.valid_from_ts
        and updated_return_lines.line_created_at < coalesce(dim_customers_history.valid_to_ts,timestamp('9999-12-31'))
    -- Point-in-time join for products based on the return date
    left join dim_products_history 
        on updated_return_lines.product_id = dim_products_history.product_id
        and updated_return_lines.line_created_at >= dim_products_history.valid_from_ts
        and updated_return_lines.line_created_at < coalesce(dim_products_history.valid_to_ts,timestamp('9999-12-31'))
    -- Point-in-time join for channels based on the return date
    left join dim_channels_history
        on updated_return_lines.channel_id = dim_channels_history.channel_id
        and updated_return_lines.line_created_at >= dim_channels_history.valid_from_ts
        and updated_return_lines.line_created_at < coalesce(dim_channels_history.valid_to_ts,timestamp('9999-12-31'))
    -- Join to the date dimension
    left join dim_date
        on cast(updated_return_lines.line_created_at as date) = dim_date.full_date
)

select * from final_returns_table