import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
import json
from gcp_analytics_sandbox.transformation.assets.dbt_project_rep import dbt_project
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition
from gcp_analytics_sandbox.shared_resources.blue_green_resource import BlueGreenDeploymentResource

# Define a custom class for translating dbt files to Dagster assets
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """
    This translator maps dbt sources to their corresponding Dagster assets.
    """
    def get_asset_key(self, dbt_resource_props):
        # This logic assumes your dbt source names match the table names
        # of the upstream assets that load data into BigQuery.
        if dbt_resource_props["resource_type"] == "source":
            # The upstream assets are named 'load_{table_name}_to_bq'
            # based on your bq_load_config.yaml
            table_name = dbt_resource_props["name"]

            if dbt_resource_props["source_name"] != "write_audit_publish":
                return dg.AssetKey(f"load_{table_name}_to_bq")
            else:
                return dg.AssetKey(table_name)

        # For dbt models, create an asset key with a 'bq_dbt' prefix
        # to distinguish them in the UI.
        else:
            # custom_asset_name =  dg.AssetKey(f"bq_dbt_{dbt_resource_props['name']}")
            return super().get_asset_key(dbt_resource_props)
        
    def get_metadata(self, dbt_resource_props):
        """
        Attaches metadata from dbt to the Dagster asset.
        """
        metadata = super().get_metadata(dbt_resource_props)
        if "meta" in dbt_resource_props:
            metadata.update(dbt_resource_props["meta"])
        return metadata
    
    def get_group_name(self, dbt_resource_props):

        # If you want to filter by model 'tags'
        # tags = dbt_resource_props.get("tags", [])

        if dbt_resource_props["resource_type"] == "source":
            return "dbt_source"
        
        elif dbt_resource_props["resource_type"] == "model":
            # Determine if the model is a staging or mart model based on its path
            model_path = dbt_resource_props.get("path") or dbt_resource_props.get("original_file_path") 
            
            if model_path:
                if "clean" in model_path:
                    return "dbt_clean"
                
                if "staging" in model_path:
                    return "dbt_staging"
                
                elif "prep" in model_path:
                    return "dbt_prep"
                
                elif "mart" in model_path:
                    return "write_audit_mart"
                
                elif "live" in model_path:
                    return "publish_live"
                
                else:
                    # Fallback for models not explicitly in staging or marts (e.g., intermediate)
                    return "other_dbt_models"
            else:
                # Fallback if path information is missing
                return "unassigned_dbt_models"
            
        elif dbt_resource_props["resource_type"] == "snapshot":
            return "dbt_snapshots"
        else:
            # For other dbt resource types like seeds, tests, etc.
            return "other_dbt_assets"

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="clean",
    partitions_def=default_partition_definition,
)
def dbt_clean_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    This asset definition runs dbt and correctly models the dependencies
    between the dbt models and the upstream BigQuery load assets.
    """
    # Pass partition information to dbt models, mainly for incremental models
    # other materializations won't need these variables
    time_window = context.partition_time_window
    dbt_vars = {
        "start_date": time_window.start.strftime('%Y-%m-%d'),
        "end_date": time_window.end.strftime('%Y-%m-%d')
    }

    # This single command works for both incremental and full-refresh models.
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], 
        context=context
    ).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="staging",
    partitions_def=default_partition_definition,
)
def dbt_staging_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    This asset definition runs dbt and correctly models the dependencies
    between the dbt models and the upstream BigQuery load assets.
    """
    # Pass partition information to dbt models, mainly for incremental models
    # other materializations won't need these variables
    time_window = context.partition_time_window
    dbt_vars = {
        "start_date": time_window.start.strftime('%Y-%m-%d'),
        "end_date": time_window.end.strftime('%Y-%m-%d')
    }

    # This single command works for both incremental and full-refresh models.
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], 
        context=context
    ).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="prep",
    partitions_def=default_partition_definition,
)
def dbt_prep_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    This asset definition runs dbt and correctly models the dependencies
    between the dbt models and the upstream BigQuery load assets.
    """
    # Pass partition information to dbt models, mainly for incremental models
    # other materializations won't need these variables
    time_window = context.partition_time_window
    dbt_vars = {
        "start_date": time_window.start.strftime('%Y-%m-%d'),
        "end_date": time_window.end.strftime('%Y-%m-%d')
    }

    # This single command works for both incremental and full-refresh models.
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], 
        context=context
    ).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="mart",
    partitions_def=default_partition_definition,
)
def dbt_mart_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource, blue_green_deployment: BlueGreenDeploymentResource):
    """
    This asset definition runs dbt and correctly models the dependencies
    between the dbt models and the upstream BigQuery load assets.
    """
    
    time_window = context.partition_time_window

    context.log.info(f"Starting WAP, writing to audit dataset: {blue_green_deployment.get_audit_dataset()}")
    
    dbt_vars = {
        "env_suffix": blue_green_deployment.get_audit_env_suffix(),
        "start_date": time_window.start.strftime('%Y-%m-%d'),
        "end_date": time_window.end.strftime('%Y-%m-%d')
    }

    # This single command works for both incremental and full-refresh models.
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], 
        context=context
    ).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="live",
    partitions_def=default_partition_definition,
)
def dbt_live_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource, blue_green_deployment: BlueGreenDeploymentResource):
    """
    This asset definition runs dbt and correctly models the dependencies
    between the dbt models and the upstream BigQuery load assets.
    """

    # whether you swap the state info of the datasets BEFORE performing the actual swap of the live views
    # or AFTER swapping the actual live views, will alter what dataset env suffix you grab
    
    context.log.info(f"Audit of marts passed, changing live views to point to new live dataset: {blue_green_deployment.get_audit_dataset()}")

    # e.g. here I'm going to perform the state update after the actual swap, so I continue to refer to 
    # the audit dataset to update the views, then I will update/swap the state for the dataset labels

    time_window = context.partition_time_window
    dbt_vars = {
        "env_suffix": blue_green_deployment.get_audit_env_suffix(),
        "start_date": time_window.start.strftime('%Y-%m-%d'),
        "end_date": time_window.end.strftime('%Y-%m-%d')
    }

    # This single command works for both incremental and full-refresh models.
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], 
        context=context
    ).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    select="resource_type:snapshot",
    name="dbt_snapshots",
    partitions_def=default_partition_definition,
)
def dbt_snapshots_asset(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    This asset definition runs dbt snapshots and correctly models their dependencies.
    """
    yield from dbt.cli(["snapshot"], context=context).stream()
