-- models/staging/stg_retail__authors_products.sql

with source as (
    select
        *
    from {{ source('boardgame_retail_raw', 'authors_products') }}
),

deduped as (
    select
        *
    from source
    
    qualify row_number() over (
        partition by author_id, product_id
        order by date_modified desc 
    ) = 1
),

renamed_and_coalesced as (
    select
        author_id,
        product_id,
        date_added as added_at,
        date_modified as modified_at,

        -- mostly a place-in, ideally more complex removal logic should be owned by the source system
        coalesce(date_deleted, date_removed) as removed_at,
        case 
            when date_deleted is not null then 'deleted'
            when date_removed is not null then 'removed'
            else null
        end as removal_reason

    from deduped
)

select * from renamed_and_coalesced 