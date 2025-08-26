-- models/marts/fct_payments.sql

{{
    config(
        materialized='incremental',
        labels = {'materialization' : 'incremental' },
        unique_key='transaction_payment_id',
        partition_by={
            "field": "payment_event_ts",
            "data_type": "timestamp",
            "granularity": "day"
            },
        cluster_by=["payment_type_sk"]
    )
}}

-- {{source('write_audit_publish', 'audit_environment_enabled')}}
with transaction_payments as (
    select 
        * 
    from {{ ref('stg_retail__transaction_payments') }}
    {% if is_incremental() %}
    where
        -- Scenario A: A variable-driven run, the default for our Dagster-partitioned dbt assets
        {% if var('start_date', false) and var('end_date', false) %}
        -- The upper bound is strict. We never process beyond the specified end date.
        date(modified_at) >= date_sub(date('{{ var("start_date") }}'), interval 3 day)
        -- We look back 3 days (can customize to w/e lateness buffer makes sense for you) from the 
        -- start of the specified window to catch any late-arriving data from the previous period.
        and date(modified_at) < date('{{ var("end_date") }}')
        -- Scenario B: A classic incremental run (no variables provided)
        {% else %}
        -- This is the fallback for runs without variables. It uses the
        -- max timestamp from the target table as its reference.
        date(modified_at) >= (
            select
                date_sub(max(source_updated_at), interval 3 day)
            from {{ this }}
        )
        {% endif %}
    {% endif %}
),

payment_types as (
    select 
        payment_type_id,
        payment_type_sk,
        valid_from_ts,
        valid_to_ts
    from {{ ref('dim_payment_types_history') }}
),

final as (
    select
        -- Primary Key
        transaction_payment_id,
        -- Surrogate Keys
        payment_types.payment_type_sk,
        -- Original Business Keys
        transaction_id,
        transaction_payments.payment_type_id,
        -- Degenerate Dimension
        payment_provider_reference,
        -- Measures
        amount_paid,
        -- Timestamps
        payment_processed_at as payment_event_ts,
        modified_at as source_updated_at
    from transaction_payments
    left join payment_types
        on transaction_payments.payment_type_id = payment_types.payment_type_id
        and transaction_payments.modified_at >= payment_types.valid_from_ts
        and transaction_payments.modified_at < coalesce(payment_types.valid_to_ts, '9999-12-31')
)

select * from final