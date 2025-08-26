-- models/staging/stg_boardgame_retail__transaction_discounts.sql

with source as (
    select
        *
    from {{ source('boardgame_retail_raw', 'transaction_discounts') }}
),

deduped as (
    select
        *
    from source
    qualify row_number() over (
        partition by transaction_discount_id
        order by date_modified desc
    ) = 1
),

renamed as (
    select
        transaction_discount_id,
        promotion_id,
        transaction_line_id,
        transaction_id,
        discounted_units,
        discount_amount,
        discount_sequence,
        date_applied as applied_at,
        date_modified as modified_at
    from deduped
)

select * from renamed