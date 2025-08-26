-- snapshots/snapshot_retail__publishers.sql

{% snapshot snapshot_retail__publishers %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='publisher_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['publisher_id']
  )
}}

select
  *
from {{ ref('cln_retail__publishers') }}

{% endsnapshot %}