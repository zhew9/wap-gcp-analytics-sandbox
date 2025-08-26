-- models/staging/deduplication/stg_retail__publishers.sql

{{
    config(
        materialized='incremental',
        unique_key='publisher_id',
        partition_by={
            "field": "date_modified",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["publisher_id"],
        on_schema_change='fail'
    )
}}


with source as (

    select 
        *
    from {{ source('boardgame_retail_raw', 'publishers') }}

    {% if is_incremental() %}
    where
        {% if var('start_date', false) and var('end_date', false) %}
        date_modified >= {{ dateadd(datepart='day',interval=-1, from_date_or_timestamp="'"~var('start_date')~"'") }}
        and date_modified < '{{ var("end_date") }}'
        {% else %}
        date_modified >= (
            select 
                {{ dateadd(datepart='day', interval=-1, from_date_or_timestamp ='max(date_modified)') }}
            from {{ this }}
            )
        {% endif %}
    {% endif %}
),

deduped as (
    select
        *
    from source
    qualify row_number() over (partition by publisher_id order by date_modified desc) = 1
)

select * from deduped
