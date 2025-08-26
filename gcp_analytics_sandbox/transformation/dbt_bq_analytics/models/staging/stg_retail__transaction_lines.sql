-- models/staging/stg_boardgame_retail__transaction_lines.sql
with source as (
    select
        *
    from {{ source('boardgame_retail_raw', 'transaction_lines') }}
),

deduped as (
    select
        *
    from source
    qualify row_number() over (
        partition by transaction_line_id 
        order by date_modified desc
    ) = 1
),

renamed as (
    select
        transaction_line_id,
        transaction_id,
        line_number,
        product_id,
        quantity,
        unit_price,
        gross_line_total,
        transaction_date as line_created_at,
        date_modified as modified_at
    from deduped
)

select * from renamed