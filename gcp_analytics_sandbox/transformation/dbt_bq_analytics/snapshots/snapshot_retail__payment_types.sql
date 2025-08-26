-- snapshots/snapshot_retail__payment_types.sql

{% snapshot snapshot_retail__payment_types %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='payment_type_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['payment_type_id']
  )
}}

select
  *
from {{ ref('cln_retail__payment_types') }}

{% endsnapshot %}