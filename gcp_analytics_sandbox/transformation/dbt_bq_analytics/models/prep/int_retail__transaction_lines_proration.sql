-- models/intermediate/int_retail__line_proration.sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_line_id',
        partition_by={
            "field": "modified_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        on_schema_change='fail'
    )
}}

with transaction_lines as (
    select
        lines.transaction_line_id,
        lines.line_number,
        lines.transaction_id,
        -- we want to enable the intermediate table to serve both sales and return fact tables
        headers.transaction_type,
        headers.original_transaction_id,
        headers.transaction_uuid,
        headers.customer_id,
        headers.channel_id,
        --headers.employee_id, -- employee_id won't be used in our project, but included as a degenerate dimension you may normally want to include
        lines.product_id,
        lines.quantity,
        lines.gross_line_total,
        headers.total_tax_amount,
        lines.line_created_at,
        lines.modified_at
    from {{ ref('stg_retail__transaction_lines') }} as lines
    left join {{ ref('stg_retail__transaction_headers') }} as headers
        on lines.transaction_id = headers.transaction_id
    {% if is_incremental() %}
     where 
        {% if var('start_date', false) and var('end_date', false) %}
        -- If variables are passed, run for a specific date range (for backfills)
        and date(lines.modified_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        and date(lines.modified_at) < date('{{ var("end_date") }}')
        {% else %}
        -- Otherwise, run for new data since the last run (standard incremental load)
        -- with a 3-day lookback to catch late-arriving data.
        date(lines.modified_at) >= (
            select
                date_sub(date(max(modified_at)), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),

line_discounts as (
    -- Aggregate discounts applied directly to each line
    select
        transaction_line_id,
        coalesce(sum(discount_amount), 0) as line_discount_amount
    from {{ ref('stg_retail__transaction_discounts') }}
    where 
        transaction_line_id is not null 
        and transaction_line_id in (select transaction_line_id from transaction_lines)
    group by 1
),

header_discounts as (
    -- Aggregate discounts applied to the entire order
    select
        transaction_id,
        coalesce(sum(discount_amount), 0) as header_discount_amount
    from {{ ref('stg_retail__transaction_discounts') }}
    where 
        transaction_id is not null
        and transaction_id in (select transaction_id from transaction_lines)
    group by 1
),

line_net as (
    -- Calculate the net total for each line after its direct discounts are applied
    select
        transaction_lines.transaction_line_id,
        transaction_lines.line_number,
        transaction_lines.transaction_id,
        transaction_lines.transaction_uuid,
        transaction_lines.transaction_type,
        transaction_lines.original_transaction_id,
        
        transaction_lines.customer_id,
        transaction_lines.channel_id,
        transaction_lines.product_id,
        transaction_lines.quantity,

        transaction_lines.gross_line_total,
        transaction_lines.total_tax_amount,
        coalesce(line_discounts.line_discount_amount, 0) as line_discount_amount,
        (transaction_lines.gross_line_total - coalesce(line_discounts.line_discount_amount, 0)) as net_line_total,
        -- original timestamps
        transaction_lines.line_created_at,
        transaction_lines.modified_at
    from transaction_lines
    left join line_discounts on transaction_lines.transaction_line_id = line_discounts.transaction_line_id
),

order_net_subtotal as (
    -- Calculate the net subtotal for the entire order before header discounts
    select
        transaction_id,
        sum(net_line_total) as pre_header_discount_subtotal
    from line_net
    group by 1
),

line_proration as (
    -- Calculate the prorated portion of the header discount for each line
    select
        line_net.*,
        -- optionally: line_net.net_line_total / nullif(order_net_subtotal.pre_header_discount_subtotal, 0) as line_proration_percentage,
        (line_net.net_line_total / nullif(order_net_subtotal.pre_header_discount_subtotal, 0)) * coalesce(header_discounts.header_discount_amount, 0) as prorated_header_discount_amount,
        (line_net.net_line_total / nullif(order_net_subtotal.pre_header_discount_subtotal, 0)) * coalesce(line_net.total_tax_amount, 0) as prorated_header_tax_amount
    from line_net
    left join order_net_subtotal on line_net.transaction_id = order_net_subtotal.transaction_id
    left join header_discounts on line_net.transaction_id = header_discounts.transaction_id
),

final_proration as (
    -- Perform the final, simple calculations using the clear names from the CTEs above
    select
        transaction_line_id,
        transaction_id,
        line_number,

        -- a chunk of degenerate dimensions because we are not building a transaction header dimension table
        -- so if we want to add these to fct_returns or fct_sales, better to include them as 
        -- degenerate dimensions instead of joining 2 potential fact tables (our headers will be another fact table)
        transaction_uuid,
        transaction_type,
        original_transaction_id,

        -- from headers
        customer_id,
        channel_id,
        -- from lines
        product_id,
        quantity,

        -- Final Measures
        gross_line_total,
        line_discount_amount,
        coalesce(prorated_header_discount_amount, 0) as prorated_discount_amount,
        (line_discount_amount + coalesce(prorated_header_discount_amount, 0)) as total_discount_amount,
        (net_line_total - coalesce(prorated_header_discount_amount, 0)) as pretax_net_amount,
        coalesce(prorated_header_tax_amount, 0) as prorated_tax_amount,
        (net_line_total - coalesce(prorated_header_discount_amount, 0) + coalesce(prorated_header_tax_amount, 0)) as net_amount,
        
        -- Timestamps
        line_created_at,
        modified_at
    from line_proration
)

select * from final_proration
