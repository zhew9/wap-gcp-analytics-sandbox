import dagster as dg

# stale & barebones: not really used atm, since most of this project involves in backfills
# might the pipeline to use sensors more later

ALL_ASSET_GROUPS = [
    "setup_bigquery", "date_dimension_generation_and_load","static_data_initialization", "dynamic_OLTP_generation", "static_ingestion_duckdb_to_gcs", "dynamic_ingestion_duckdb_to_gcs", 
    "static_ingestion_gcs_to_bq", "dynamic_ingestion_gcs_to_bq", "dbt_clean", "dbt_staging", "dbt_prep", "dbt_mart", "dbt_live", "dbt_snapshots"
]

list_sensors_by_group = []
for asset_group in ALL_ASSET_GROUPS:
    list_sensors_by_group.append(
        dg.AutomationConditionSensorDefinition(
            f"{asset_group}_sensor", target=dg.AssetSelection.groups(asset_group)
        )
    )

