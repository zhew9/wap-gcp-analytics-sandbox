-- models/staging/stg_boardgame_retail__transaction_payments.sql

with source as (
    select
        *
    from {{ source('boardgame_retail_raw', 'transaction_payments') }}
),

deduped as (
    select
        *
    from source

    qualify row_number() over (
        partition by transaction_payment_id 
        order by date_modified desc
    ) = 1
),

renamed as (
    select
        transaction_payment_id,
        transaction_id,
        payment_type_id,
        amount_paid,
        payment_provider_reference,
        payment_date as payment_processed_at,
        date_modified as modified_at
    from deduped
)

select * from renamed