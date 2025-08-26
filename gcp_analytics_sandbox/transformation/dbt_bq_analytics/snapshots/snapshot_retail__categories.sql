{% snapshot snapshot_retail__categories %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='category_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['category_id']
  )
}}

select
  *
from {{ ref('cln_retail__categories') }}

{% endsnapshot %}