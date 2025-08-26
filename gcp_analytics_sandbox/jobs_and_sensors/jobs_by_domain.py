from dagster import define_asset_job, AssetSelection
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition


setup_bigquery_job = define_asset_job(
        name="step_1_setup_bigquery_job",
        selection=AssetSelection.groups("setup_bigquery"),
        description="Runs the asset for creating the BigQuery datasets we will be using",
    )

# both static w/upstream() and date_dim assets but not setup_bigquery
static_selection = (AssetSelection.groups("static_ingestion_gcs_to_bq").upstream() | AssetSelection.groups("date_dimension_generation_and_load") ) - AssetSelection.groups("setup_bigquery")
static_data_ingestion_job = define_asset_job(
        name="step_2_static_data_ingestion_job",
        selection=static_selection,
        description="""Runs the one-time assets that setup our local DuckDB, generates a Date Dimension table with localized holidays, loads in tables from provided default parquet files, and then runs incremental load ingestion
            of all of those tables to GCS and append-only load into BigQuery: does a one time run of all assets in the ['static_data_initialization', 'static_ingestion_duckdb_to_gcs', 'static_ingestion_gcs_to_bq', 'date_dimension_generation_and_load'] groups.""",
        partitions_def=default_partition_definition,
    )

dynamic_generation_selection = AssetSelection.groups("dynamic_OLTP_generation")

dynamic_generation_job = define_asset_job(
        name="step_3_dynamic_data_generation_job",
        selection=dynamic_generation_selection,
        description="""Runs the repeat, partitioned assets that repeatedly generates mock data for OLTP tables in our local DuckDB, 
        broken up into two parts because of Dagster limitations of not having consistent partition range logic between Jobs and regular backfills:
        these assets were changed to fit Job partitioning logic (single run backfills) compared to the original multi-run backfill logic (a bandaid)
        """,
        partitions_def=default_partition_definition,
)

dynamic_selection = (AssetSelection.groups("dynamic_ingestion_gcs_to_bq").upstream() - (AssetSelection.groups("static_ingestion_gcs_to_bq").upstream() | AssetSelection.groups("dynamic_OLTP_generation") | AssetSelection.groups("setup_bigquery")))
dynamic_data_ingestion_job = define_asset_job(
        name="step_3_part2_dynamic_data_ingestion_job",
        selection=dynamic_selection,
        description="""Loads each table incrementally to GCS and append-only load into BigQuery 
            Runs all assets in the ['dynamic_OLTP_generation', 'dynamic_OLTP_generation', 'dynamic_ingestion_gcs_to_bq'] groups.
            """,
        partitions_def=default_partition_definition,
    )

upstream_of_dbt = (AssetSelection.groups("dynamic_ingestion_gcs_to_bq").upstream() | AssetSelection.groups("static_ingestion_gcs_to_bq").upstream() | AssetSelection.groups("setup_bigquery") | AssetSelection.groups("date_dimension_generation_and_load"))
clean_staging_prep_selection = (AssetSelection.groups("dbt_staging").upstream() | AssetSelection.groups("dbt_prep").upstream() )

pre_WAP_job = define_asset_job(
        name="step_4_pre_WAP_dbt_transformations",
        selection=(clean_staging_prep_selection - upstream_of_dbt),
        description="""Runs all dbt transformation for the (clean, snapshots, staging, prep) layers before Write-Audit-Publish/blue green deployment."""
    )

setup_wap_selection = AssetSelection.groups("setup_audit_WAP")

setup_audit_WAP_job = define_asset_job(
        name="step_5_setup_WAP_blue_green_deployment",
        selection=setup_wap_selection,
        description="""Initates WAP/blue green deployment: gets the current deployment status of each dataset, then for the audit dataset it
        cleans/deletes the old/stale tables (if any), then performs zero-copy clones of current mart incremental tables in preparation of transformations."""
    ) 

write_audit_selection = (AssetSelection.groups("write_audit_mart").upstream())
write_audit_WAP_job = define_asset_job(
        name="step_6_audit_dbt_transformations",
        selection=(write_audit_selection - (clean_staging_prep_selection | setup_wap_selection)),
        description="""Runs dbt transformations and tests for the audit mart layer."""
    ) 

publish_selection = AssetSelection.groups("publish_live").upstream()
publish_job = define_asset_job(
        name="step_7_publish_live_dbt_transformations",
        selection=(publish_selection - (write_audit_selection | setup_wap_selection | clean_staging_prep_selection | upstream_of_dbt)),
        description="""Promotes the audit dataset to live, by transforming the source of downstream views."""
    ) 

post_publish_selection = AssetSelection.assets("update_live_dataset_status")
post_publish_job = define_asset_job(
        name="step_8_post_publish_update_and_cleanup",
        selection=post_publish_selection,
        description="""Updates the dataset labels, optionally drops contents of previous live dataset to save storage costs."""
    ) 

list_jobs_by_domain = [
    setup_bigquery_job,
    static_data_ingestion_job,
    dynamic_generation_job,
    dynamic_data_ingestion_job,
    pre_WAP_job,
    setup_audit_WAP_job,
    write_audit_WAP_job,
    publish_job,
    post_publish_job
]


