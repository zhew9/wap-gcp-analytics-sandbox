import dagster as dg
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound
from gcp_analytics_sandbox.transformation.assets.dbt_project_rep import dbt_project
from dagster_dbt import get_asset_key_for_model
from pathlib import Path
from datetime import datetime
import json
from gcp_analytics_sandbox.transformation.assets.dbt_build_snapshots_and_models import dbt_staging_assets, dbt_prep_assets, dbt_mart_assets, dbt_live_assets
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition
from gcp_analytics_sandbox.shared_resources.blue_green_resource import BlueGreenDeploymentResource
import time

# helper function for compiling a list of dbt_assets, from dbt models, as dependencies for regular Dagster assets
def get_dbt_model_names_by_folder(dbt_manifest_path: Path, dbt_assets_definition: dict[str, dg.AssetsDefinition]) -> list[dg.AssetKey]:
    """
    Parses the dbt manifest.json file to find all models in a specific folder
    and returns their corresponding Dagster asset metadata_keys.
    """
    with open(dbt_manifest_path, "r") as f:
        manifest_data = json.load(f)
        asset_keys = []
        for _, node_info in manifest_data["nodes"].items():
            if node_info["resource_type"] == "model":
            # Check if the model is in the specified folder
                for model_folder, asset_definition in dbt_assets_definition.items():
                    if node_info["path"].startswith(model_folder):
                        # The asset key for a dbt model is its model name
                        model_name = node_info["name"]
                        if model_name:
                            asset_keys.append(get_asset_key_for_model([asset_definition], model_name))
        
        return asset_keys


def delete_tables_from_bigquery_dataset(dataset_to_empty: str, bigquery: BigQueryResource, context: dg.AssetExecutionContext) -> list[str]:
    """
    Modular function for deleting the tables of a dataset, used for setup and cleanup for diff 
    blue/green WAP stages - mainly to save storage costs for divierging zero-copy clones.

    Takes in: a BigQuery dataset id, and the assets BQ resource and execution context 

    Returns a list of table names that have been deleted from the dataset.
    """

    with bigquery.get_client() as bq_client:

        dataset_ref = bq.DatasetReference(project=bq_client.project, dataset_id=dataset_to_empty)

        context.log.info(f"Preparing to delete tables in dataset : {dataset_ref}")
        # a list of TableListItem
        table_iterator = list(bq_client.list_tables(dataset=dataset_ref))
        tables_deleted = []

        for table in table_iterator:
            try:
                # full_table_id is an inherited property from a parent class of TableListItem
                context.log.info(f"Attempting to delete {table.full_table_id}")
                bq_client.delete_table(table=table, not_found_ok=True)
                tables_deleted.append(table.full_table_id)
            except Exception as e:
                raise Exception(f"Error deleting {table.full_table_id}: {e}")

        context.log.info(f"done deleting tables in {dataset_to_empty}")
    
    return tables_deleted

#helper function for recreate_bigquery_dataset(), logs and polls until the dataset is confirmed to be deleted
def _poll_for_deletion(
    bq_client: bq.Client,
    context: dg.OpExecutionContext,
    dataset_ref: bq.DatasetReference,
    timeout: int = 60,
) -> None:
    context.log.info(f"Polling to confirm deletion of '{dataset_ref.dataset_id}'...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            bq_client.get_dataset(dataset_ref)
            context.log.info(f"Dataset '{dataset_ref.dataset_id}' still exists. Waiting...3s")
            time.sleep(3)
        except NotFound:
            context.log.info(f"Successfully confirmed dataset '{dataset_ref.dataset_id}' is deleted.")
            return
    # If the loop completes without a NotFound error, it timed out.
    raise TimeoutError(
        f"Timed out after {timeout}s waiting for '{dataset_ref.dataset_id}' to be deleted."
    )

def recreate_bigquery_dataset(
    context: dg.OpExecutionContext,
    bq_resource: BigQueryResource,
    dataset_id: str
) -> bq.Dataset:
    """
    Deletes then recreates a given BigQuery dataset and returns the newly updated dataset.

    - Copies current dataset metadata, 
    - deletes the dataset (and wait for asynch delete to finish) 
    - Recreate dataset with copied metadata, and verifies metadata is properly set
    """
    context.log.info(f"--- Starting recreation process for dataset: {dataset_id} ---")

    try:
        with bq_resource.get_client() as bq_client:

            """
            # if your datasets are setup with the labels, you could just grab the dataset by the labels
            
            label_filter = labels.deployment_status:live
            datasets = list(bq_client.list_datasets(filter=label_filter))
            """
            # fetch and save metadata
            context.log.info("Step 1: Fetching original dataset metadata...")
            dataset_ref = bq.DatasetReference(bq_client.project, dataset_id)
            original_dataset = bq_client.get_dataset(dataset_ref)

            saved_metadata = {}
            # This is a predefined list of attributes we know we can set, you can add/remove some
            # this is also a good candidate for taking configs from a file
            settable_attributes = [
                "labels", "description", "access_entries", 
                "default_table_expiration_ms", "location",
            ]
            for attr_name in dir(original_dataset):
                # Filter for only the attributes we can set and ignore private/internal ones
                if attr_name in settable_attributes:
                    saved_metadata[attr_name] = getattr(original_dataset, attr_name)
                    context.log.info(f"Fetched '{attr_name}' metadata for dataset '{dataset_id}'.")

            # deletion
            try:
                context.log.info("Step 2: Initiating dataset deletion...")
                # asynch delete
                bq_client.delete_dataset(dataset_ref, delete_contents=True)
                #polling, waiting for the deletion to finish
                _poll_for_deletion(bq_client, context, dataset_ref)
            except Exception as e:
                context.log.error(f"Failed during the DELETION phase for '{dataset_id}': {e}")
                raise dg.Failure(
                    description=f"Dataset deletion failed for {dataset_id}. See logs for details."
                ) from e

            # creation and verification
            try:
                context.log.info("Step 3: Recreating dataset with saved metadata...")
                # create using original ref
                newly_created_dataset = bq_client.create_dataset(dataset_ref)
                metadata_keys = []
                #set metadata from saved copy
                for key, value in saved_metadata.items():
                    if value is not None:
                        setattr(newly_created_dataset, key, value)
                        metadata_keys.append(key)

                # update the dataset and get the newly updated state
                modified_dataset = bq_client.update_dataset(newly_created_dataset, metadata_keys)
                context.log.info("Step 4: Verifying restored metadata...")

                mismatches = []
                # verify the updated metadata
                for key, saved_value in saved_metadata.items():
                    current_value = getattr(modified_dataset, key)
                    if current_value != saved_value:
                        mismatches.append(key)
                        context.log.error(f"  - Mismatch for '{key}':")
                        context.log.error(f"    Expected: {saved_value}")
                        context.log.error(f"    Got: {current_value}")
                if mismatches:
                    raise ValueError(f"Metadata verification failed! Mismatched metadata_keys: {mismatches}")
                
                context.log.info("Verification successful: All saved metadata attributes match.")

            except Exception as e:
                context.log.error(f"Failed during the CREATION or VERIFICATION phase for '{dataset_id}': {e}")
                raise dg.Failure(
                    description=f"Dataset creation or verification failed for {dataset_id}.",
                    metadata={
                        "error_message": e,
                        "dataset_id": dataset_id,
                    }
                ) from e

    except NotFound as e:
        context.log.warning(f"Dataset '{dataset_id}' not found at the start. Aborting operation.")
        raise dg.Failure(
            description=f"Cannot recreate dataset '{dataset_id}' because it does not exist.",
            metadata={
                "error_message": e,
                "dataset_id": dataset_id,
            }
        ) from e
    
    except Exception as e:
        context.log.error(f"An unexpected setup error occurred: {e}")
        raise dg.Failure(
            description=f"Dataset deletion failed for {dataset_id}. See logs for details.",
            metadata={
                "error_message": e,
                "dataset_id": dataset_id,
            }
        ) from e

    return modified_dataset

@dg.asset(
    name="get_current_deployment_state",
    kinds={"gcp", "bigquery"},
    group_name="setup_audit_WAP",
    # deps=get_dbt_model_names_by_folder(dbt_project.manifest_path, {"staging": dbt_staging_assets, "prep": dbt_prep_assets}),
)
def get_current_deployment_state(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource) -> dg.MaterializeResult:
    """
    This asset determines the current live dataset in our blue/green deployment by querying
    both blue and green dataset's labels to determine which dataset is the live dataset and which is the
    auditing dataset for Write-Audit-Publish.
    """
    # fetch state from live datasets
    blue_green_deployment.update_state_from_bq(context)

    audit_dataset = blue_green_deployment.get_audit_dataset()
    audit_env_suffix = blue_green_deployment.get_audit_env_suffix()
    live_dataset = blue_green_deployment.get_live_dataset()
    live_env_suffix = blue_green_deployment.get_live_env_suffix()

    context.log.info(f"current live_dataset: {live_dataset}, current live_env_suffix: {live_env_suffix}")
    context.log.info(f"current audit_dataset: {audit_dataset}, current audit_env_suffix: {audit_env_suffix}")

    return dg.MaterializeResult(
        metadata={
            "current_live_dataset": live_dataset,
            "current_audit_dataset": audit_dataset,
        }
    )


@dg.asset(
    name="clear_audit_dataset",
    kinds={"gcp", "bigquery"},
    group_name="setup_audit_WAP",
    deps=["get_current_deployment_state"],
)
def clear_audit_dataset(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource) -> dg.MaterializeResult:
    """
        Cleans the audit dataset of it's contents, either deleting the dataset itself then recreating a copy OR
        deleting all the contents/tables in the dataset. Can be used either for Pre-Audit or Post-Publish cleanup.
    """

    # get the current audit dataset
    current_audit_dataset = blue_green_deployment.get_audit_dataset()
    # delete/recreate: copy current dataset metadata, delete dataset, wait for asynch delete, then recreate dataset with copied metadata
    new_dataset = recreate_bigquery_dataset(context, bigquery, current_audit_dataset)

    # alternative: if you want to delete the tables in the dataset instead of deleting the dataset itself
    # tables_deleted = delete_tables_from_bigquery_dataset(current_audit_dataset, bigquery, context)

    # optional, update local dataset metadata from online (optional: prev function also checks for matching metadata)
    # blue_green_deployment.update_state_from_bq(context)

    return dg.MaterializeResult(
        metadata={
            "dataset_deleted_and_recreated": current_audit_dataset,
            "recreated_dataset_labels": new_dataset.labels,
        }
    )

@dg.asset(
    name="clone_live_to_audit",
    kinds={"gcp", "bigquery"},
    group_name="setup_audit_WAP",
    deps=["clear_audit_dataset"],
)
def clone_live_to_audit(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource) -> dg.MaterializeResult:
    """
    This asset uses zero-copy cloning of the incremental tables (determined by a label) of the 
    current live dataset of our blue/green deployment cycle to the auditing dataset, in preparation for testing and promotion.
    for writing transformations
    """    
    # filter for all the tables in the live dataset, with an incremental label (either set during creation in dbt models or added later)
    #   - if you have a lot of tables, this step you might want to just query the dataset's INFORMATION_SCHEMA.TABLE_OPTIONS to save time
    # if the live dataset doesn't have any incremental tables to clone, then it won't clone any
    
    # then for each table with matching labels, zero-copy clone over the table from live to audit

    live_dataset = blue_green_deployment.get_live_dataset()
    audit_dataset = blue_green_deployment.get_audit_dataset()
    source_dataset_id = live_dataset
    destination_dataset_id = audit_dataset

    context.log.info(f"Safely finding incremental tables in '{source_dataset_id}'...")
    label_key_to_find = "materialization"
    label_value_to_find = "incremental"

    try:
        with bigquery.get_client() as bq_client:
            # an alternative is to query the dataset's INFORMATION_SCHEMA.TABLE_OPTIONS

            # getting all tables then filtering them for labels
            list_tables = list(bq_client.list_tables(source_dataset_id))
            tables_to_clone = []
            for table in list_tables:
                # context.log.info(f" look at labels for: {table.full_table_id}")
                if table.labels.get(label_key_to_find) == label_value_to_find:
                    context.log.info(f"found label '{label_value_to_find} : {label_value_to_find}' for table '{table.table_id}' in dataset '{source_dataset_id}'")
                    tables_to_clone.append(table.table_id)
            
            context.log.info(f"Found {len(tables_to_clone)} tables: {tables_to_clone}")

            tables_cloned = []
            for table_id in tables_to_clone:
                source_table_ref = f"{source_dataset_id}.{table_id}"
                dest_table_ref = f"{destination_dataset_id}.{table_id}"
                try:
                    clone_query = f"CREATE OR REPLACE TABLE `{dest_table_ref}` CLONE `{source_table_ref}`;"
                    clone_job = bq_client.query(clone_query)
                    clone_job.result()
                    context.log.info(f"Successfully cloned '{table_id}' from '{source_dataset_id}' to '{destination_dataset_id}'.")
                    tables_cloned.append(table_id)
                except Exception as e:
                    raise dg.Failure(
                        description=f"Failed to clone '{table_id}' from '{source_dataset_id}' to '{destination_dataset_id}'.",
                        metadata={
                            "error_message": e,
                            "source_dataset_id": source_dataset_id,
                            "destination_dataset_id": destination_dataset_id,
                            "table_id": table_id,
                        }
                    ) from e


            context.log.info(f"successfully cloned {len(tables_cloned)} incremental tables: {tables_cloned}")

            return dg.MaterializeResult(
                metadata={
                    "tables_to_clone": tables_to_clone,
                    "tables_cloned": tables_cloned,
                    "live_dataset": live_dataset,
                    "audit_dataset": audit_dataset,
                    "source_dataset": source_dataset_id,
                    "destination_dataset": destination_dataset_id,
                }
            )
        
    except Exception as e:
        context.log.error(f"Failed to clone incremental tables from '{source_dataset_id}': {e}")
        raise dg.Failure(
            description=f"Could not clone incremental tables from: '{source_dataset_id}'."
        ) from e

@dg.asset(
    name="audit_environment_enabled",
    kinds={"gcp", "bigquery"},
    group_name="setup_audit_WAP",
    deps=["clone_live_to_audit"],
)
def audit_environment_enabled(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource) -> dg.MaterializeResult:
    """
    This is mainly a checkpoint asset, for denoting pre-write setup.
    """
    return dg.MaterializeResult(
        metadata={
            "live_dataset": blue_green_deployment.get_live_dataset(),
            "audit_dataset": blue_green_deployment.get_audit_dataset(),
        }
    )


@dg.asset(
    name="audit_dataset_validated",
    kinds={"gcp", "bigquery"},
    group_name="setup_publish_WAP",
    deps=get_dbt_model_names_by_folder(dbt_project.manifest_path, {"mart": dbt_mart_assets}),
)
def audit_dataset_validated(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource):
    """
    This is mainly a checkpoint asset, for showing the requirement of mart audits passing
    """
    return


@dg.asset(
    name="update_live_dataset_status",
    kinds={"gcp", "bigquery"},
    group_name="post_publish_cleanup",
    deps=get_dbt_model_names_by_folder(dbt_project.manifest_path, {"live": dbt_live_assets}),
)
def update_live_dataset_status(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource) -> dg.MaterializeResult:
    """
    After view promotion, update deployment state metadata/dataset labels to reflect promotion and demotion
    """
    old_audit_dataset = blue_green_deployment.get_audit_dataset()
    old_live_dataset = blue_green_deployment.get_live_dataset()
    # update dataset labels
    context.log.info(f"Promoting previous audit dataset: {old_audit_dataset} to live status")
    blue_green_deployment.promote_audit_to_live(context)
    context.log.info(f"Promotied previous audit dataset: {old_audit_dataset} to new live dataset status")
    context.log.info(f"Demoted previous live dataset: {old_live_dataset} to new audit dataset status")
    # re-fetch updates and compare
    context.log.info(f"Updating dataset labels to reflect promotion")
    blue_green_deployment.update_state_from_bq(context)

    audit_dataset = blue_green_deployment.get_audit_dataset()
    live_dataset = blue_green_deployment.get_live_dataset()  

    return dg.MaterializeResult(
        metadata={
            "previous_live_dataset": old_live_dataset,
            "previous_audit_dataset": old_audit_dataset,
            "current_dataset_promoted_to_live": live_dataset,
            "current_dataset_demoted_to_audit": audit_dataset,
        }
    )


@dg.asset(
    name="drop_stale_dataset_tables",
    kinds={"gcp", "bigquery"},
    group_name="post_publish_cleanup",
    deps=["update_live_dataset_status"],
)
def drop_stale_dataset_tables(context: dg.AssetExecutionContext, bigquery: BigQueryResource, blue_green_deployment: BlueGreenDeploymentResource) -> dg.MaterializeResult:
    """
    OPTIONAL: if you want to cleanup the contents of the stale, previous live_dataset BEFORE
    the next WAP iteration (where the next audit dataset, which is this stale dataset, is cleared 
    before cloning over the fresh live_dataset). 
    
    Mainly to save storage costs for divierging zero-copy table clones.
    """
    # stale/previous live dataset is now the labeled as the audit dataset
    current_audit_dataset = blue_green_deployment.get_audit_dataset()
    # drop and recreate the dataset with same metadata
    cleaned_dataset = recreate_bigquery_dataset(context, bigquery, current_audit_dataset)

    return dg.MaterializeResult(
        metadata={
            "dataset_cleaned": cleaned_dataset,
        }
    )