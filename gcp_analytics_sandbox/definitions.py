from dagster import Definitions, load_assets_from_modules
from pathlib import Path

# Load all assets from modules
from gcp_analytics_sandbox.initialization.assets import create_bq_datasets, generate_dynamic_mock_data, generate_load_date_dim, initialize_static_data
from gcp_analytics_sandbox.ingestion.assets import assets_gcs_load, assets_bq_load
from gcp_analytics_sandbox.transformation.assets import dbt_build_snapshots_and_models, revert_bq_table_example, write_audit_publish_cycle

# Load all resources from modules
from gcp_analytics_sandbox.shared_resources.duckdb_resources import read_local_duckdb_resource, write_local_duckdb_resource
from gcp_analytics_sandbox.shared_resources.gcp_resources import gcs_resource, bq_resource
from gcp_analytics_sandbox.shared_resources.dbt_cli_resource import dbt_bq_analytics_resource
from gcp_analytics_sandbox.shared_resources.blue_green_resource import BlueGreenDeploymentResource

# Load all jobs and sensors
from gcp_analytics_sandbox.jobs_and_sensors.jobs_by_domain import list_jobs_by_domain
from gcp_analytics_sandbox.jobs_and_sensors.sensors_by_group import list_sensors_by_group

# Load all assets from modules
module_assets = load_assets_from_modules([
    assets_gcs_load,
    create_bq_datasets, 
    assets_bq_load, 
    initialize_static_data, 
    generate_dynamic_mock_data, 
    generate_load_date_dim,
    dbt_build_snapshots_and_models,
    revert_bq_table_example,
    write_audit_publish_cycle
])

# Initiate Resource here, because it depends on another resource
blue_green_deployment = BlueGreenDeploymentResource(
    bigquery=bq_resource,
    config_path=str(Path(__file__).parent / "shared_resources" / "blue_green_wap_deployment.yaml"),
    default_live="boardgame_retail_mart_blue",
    default_audit="boardgame_retail_mart_green",
)

# Define the Dagster repository
defs = Definitions(
    assets=module_assets,
    jobs=[*list_jobs_by_domain],
    sensors=[*list_sensors_by_group],
    resources={
        # we have read and write separate resources because there is a 
        # concurrency limit of 1 for writing to local DuckDB 
        "read_duckdb": read_local_duckdb_resource,
        "write_duckdb": write_local_duckdb_resource,
        "gcs": gcs_resource, 
        "bigquery": bq_resource,
        "dbt": dbt_bq_analytics_resource,
        "blue_green_deployment": blue_green_deployment,
    },
)