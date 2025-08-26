-- models/staging/stg_boardgame_retail__transaction_headers.sql
with source as (
    select
        *
    from {{ source('boardgame_retail_raw', 'transaction_headers') }}
),

deduped as (
    select
        *
    from source
    qualify row_number() over (
        partition by transaction_id
        order by date_modified desc
    ) = 1
),

renamed as (
    select
        transaction_id,
        transaction_uuid,
        channel_id,
        employee_id, -- can be pretty much ignored for our project, we're not modeling employees
        customer_id,
        gross_subtotal_amount,
        total_tax_amount,
        total_amount,
        transaction_type,
        original_transaction_id,
        transaction_date as transaction_created_at,
        date_modified as modified_at
    from deduped
)

select * from renamed