-- snapshots/snapshot_retail__promotions.sql

{% snapshot snapshot_retail__promotions %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='promotion_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['promotion_id']
  )
}}

select
  *
from {{ ref('cln_retail__promotions') }}

{% endsnapshot %}