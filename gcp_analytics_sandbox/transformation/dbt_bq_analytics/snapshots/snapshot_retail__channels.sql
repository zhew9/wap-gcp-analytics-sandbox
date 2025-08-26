{% snapshot snapshot_retail__channels %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='channel_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['channel_id']
  )
}}

select
  *
from {{ ref('cln_retail__channels') }}

{% endsnapshot %}