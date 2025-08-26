-- snapshots/snapshot_retail__products.sql

{% snapshot snapshot_retail__products %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='product_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['product_id']
  )
}}

select
  *
from {{ ref('cln_retail__products') }}

{% endsnapshot %}