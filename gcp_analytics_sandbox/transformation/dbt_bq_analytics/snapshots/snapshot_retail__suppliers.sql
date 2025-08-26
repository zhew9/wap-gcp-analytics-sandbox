-- snapshots/snapshot_retail__suppliers.sql

{% snapshot snapshot_retail__suppliers %}

{{
  config(
    target_schema='boardgame_retail_snapshots',
    unique_key='supplier_id',
    strategy='timestamp',
    updated_at='date_modified',
    partition_by={
            "field": "dbt_updated_at",
            "data_type": "timestamp",
            "granularity": "day"
    },
    cluster_by=['supplier_id']
  )
}}

select
  *
from {{ ref('cln_retail__suppliers') }}

/* if your source system somehow doesn't have consistent date_modified and soft delete handling, 
even if a delete was the last event to modify the record, then do:

select
  greatest(date_modified, coalesce(date_deleted, '1970-01-01')) as last_modified_at,
  *
from {{ ref('cln_retail__suppliers') }}

-- change the 'updated_at' config to 'last_modified_at'
*/

{% endsnapshot %}