-- models/mart/sales/fct_transactions.sql

{{
    config(
        materialized='incremental',
        labels={'materialization': 'incremental'},
        partition_by={
            "field": "line_created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["customer_sk", "channel_sk"],
        unique_key='transaction_id',
        on_schema_change='fail'
    )
}}

-- 1. Identify the transaction_ids that have been modified within the incremental window.
-- This includes both new transactions and older transactions affected by recent returns.
-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with transactions_to_update as (
    select
        distinct transaction_id
    from {{ ref('int_retail__transaction_lines_proration') }}
    {% if is_incremental() %}
    -- This is the user-provided, correct incremental logic.
    where
        {% if var('start_date', false) and var('end_date', false) %}
        date(modified_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        and date(modified_at) < date('{{ var("end_date") }}')
        {% else %}
        date(modified_at) >= (
            select
                date_sub(max(modified_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),

-- 2. Fetch ALL line items associated with the transaction_ids identified above.
-- This is the crucial step that pulls both old purchase lines and new return lines,
-- ensuring a complete and correct recalculation.
int_proration_for_update as (
    select
        transaction_id,
        transaction_type,
        transaction_uuid,

        -- measures needed for aggregation
        gross_line_total,
        total_discount_amount,
        prorated_tax_amount,
        net_amount,
        
        -- degenerate dimensions
        original_transaction_id,
        customer_id,
        channel_id,

        -- timestamps
        line_created_at,
        modified_at
    from {{ ref('int_retail__transaction_lines_proration') }}
    where transaction_id in (select transaction_id from transactions_to_update)
),

-- 3. Create a unified set of measures where returns are represented as negative values.
unioned_measures as (
    select
        transaction_id,
        gross_line_total,
        total_discount_amount,
        prorated_tax_amount,
        net_amount
    from int_proration_for_update
    where transaction_type = 'Purchase'

    union all

    select
        transaction_id,
        gross_line_total * -1 as gross_line_total,
        total_discount_amount * -1 as total_discount_amount,
        prorated_tax_amount * -1 as prorated_tax_amount,
        net_amount * -1 as net_amount
    from int_proration_for_update
    where transaction_type = 'Return'
),

-- 4. Aggregate the measures to the transaction_id grain.
aggregated_measures as (
    select
        transaction_id,
        sum(gross_line_total) as gross_subtotal_amount,
        sum(total_discount_amount) as total_discount_amount,
        sum(prorated_tax_amount) as total_tax_amount,
        sum(net_amount) as net_amount
    from unioned_measures
    group by 1
),

-- 5. Get the descriptive, header-level attributes for the transactions being updated.
header_attributes as (
    select
        transaction_id,
        min(transaction_uuid) as transaction_uuid,
        min(transaction_type) as transaction_type,
        min(original_transaction_id) as original_transaction_id,
        min(customer_id) as customer_id,
        min(channel_id) as channel_id,
        min(line_created_at) as line_created_at,
        max(modified_at) as modified_at
    from int_proration_for_update
    group by 1
),

-- Dimension tables for joining
dim_customers_history as (
    select 
        customer_id,
        customer_sk,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_customers_history') }}
),
dim_channels_history as (
    select
        channel_id,
        channel_sk,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_channels_history') }}
),
dim_date as (
    select
        date_key,
        full_date
    from {{ ref('dim_date') }}
)

-- 6. Final join to assemble the fact table rows for the incremental batch.
select
    -- Primary Key & Degenerate Dimensions
    header_attributes.transaction_id,
    header_attributes.transaction_uuid,
    header_attributes.transaction_type,
    header_attributes.original_transaction_id,

    -- Surrogate Keys from Conformed Dimensions
    coalesce(safe_cast(dim_customers_history.customer_sk as string), '-1') as customer_sk,
    coalesce(safe_cast(dim_channels_history.channel_sk as string), '-1') as channel_sk,
    coalesce(safe_cast(dim_date.date_key as string), '-1') as transaction_date_key,

    -- Final Aggregated Measures
    coalesce(aggregated_measures.gross_subtotal_amount, 0) as gross_subtotal_amount,
    coalesce(aggregated_measures.total_discount_amount, 0) as total_discount_amount,
    coalesce(aggregated_measures.total_tax_amount, 0) as total_tax_amount,
    coalesce(aggregated_measures.net_amount, 0) as net_amount,
    (coalesce(aggregated_measures.net_amount, 0) + coalesce(aggregated_measures.total_tax_amount, 0)) as total_amount,

    -- Timestamps
    header_attributes.line_created_at,
    header_attributes.modified_at

from header_attributes
left join aggregated_measures
    on header_attributes.transaction_id = aggregated_measures.transaction_id
left join dim_customers_history
    on header_attributes.customer_id = dim_customers_history.customer_id
    and header_attributes.line_created_at >= dim_customers_history.valid_from_ts
    and header_attributes.line_created_at < coalesce(dim_customers_history.valid_to_ts, '9999-12-31')
left join dim_channels_history
    on header_attributes.channel_id = dim_channels_history.channel_id
    and header_attributes.line_created_at >= dim_channels_history.valid_from_ts
    and header_attributes.line_created_at < coalesce(dim_channels_history.valid_to_ts, '9999-12-31')
left join dim_date
    on cast(header_attributes.line_created_at as date) = dim_date.full_date