import dagster as dg
from dagster_gcp import BigQueryResource
# Import specific exceptions from the Google Cloud client libraries
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest, GoogleAPICallError

# This an asset factory example, in case you want build it for specific assets/pass some defaults

def build_revert_asset(asset_key: dg.AssetKey, dataset_id: str, table_id: str , default_revert_mins: int):
    """
    This factory function creates a manual Dagster asset that can revert
    a BigQuery table to a previous state using Time Travel.

    Args:
        asset_key (dg.AssetKey): The AssetKey of the dbt model to revert.

        default_revert_mins (int): The default number of minutes to go back in time. Must be within
        your BigQuery dataset's configured Time Travel window. You can also customize it in the UI config.

    """

    @dg.asset(
        name=f"revert_{dataset_id}.{table_id}",
        group_name="revert_table_example",
        kinds={"bigquery"},
        config_schema={
            "revert_minutes": dg.Field(
                dg.Int,
                default_value=default_revert_mins,
                description=(
                    "The number of minutes to go back in time for the revert. "
                    "Must be within your BigQuery dataset's Time Travel window (default 7 days)."
                ),
            )
        },
        deps=asset_key
    )
    def _revert_asset(context: dg.AssetExecutionContext, bigquery: BigQueryResource):
        """
        A Dagster asset that executes a BigQuery Time Travel query to revert an audit table.
        In case an audit table does not pass a data quality check during a WAP workflow.
        """

        revert_minutes = context.op_config["revert_minutes"]
        # The project ID is correctly retrieved from the configured resource
        project_id = bigquery.project

        original_table_ref = f"`{project_id}.{dataset_id}.{table_id}`"
        restored_table_ref = f"`{project_id}.{dataset_id}.{table_id}_restored`"

        revert_script = f"""
        -- Step 1: Create a new table from the backup point.
        CREATE OR REPLACE TABLE {restored_table_ref} AS
        SELECT * FROM {original_table_ref}
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {revert_minutes} MINUTE);

        -- Step 2: Drop the original, problematic table.
        DROP TABLE {original_table_ref};

        -- Step 3: Rename the restored table to have the original's name.
        ALTER TABLE {restored_table_ref} RENAME TO `{table_id}`;
        """

        context.log.info(f"Executing restoration copy query for table: {original_table_ref}")

        with bigquery.get_client() as client:
            # Use a try...except block for robust error handling
            try:
                # .result() waits for the job to complete and raises an error on failure
                query_job = client.query(revert_script)
                query_job.result()
                context.log.info(f"Successfully reverted {table_id}.")
        
            except (BadRequest, GoogleAPICallError, NotFound) as e:
                # Consolidate error logging for any potential failure during the script execution.
                context.log.error(
                    f"Failed to revert {original_table_ref}. "
                    f"Check permissions, table existence, and Time Travel validity. Original error: {e}"
                )
                # Re-raise the exception to ensure the Dagster run fails correctly.
                raise
            
            finally:
                # This block runs whether the try block succeeded or failed.
                # It cleans up the temporary table if it was left behind after a failure.
                context.log.info(f"Checking for and cleaning up temporary table {restored_table_ref}...")
                try:
                    # This command will succeed if the temp table exists and fail with NotFound if it doesn't.
                    client.get_table(restored_table_ref.strip("`")) # get_table does not want backticks
                    
                    # If the above line didn't fail, the temp table exists and must be dropped.
                    context.log.warning(f"Orphaned temporary table {restored_table_ref} found. Deleting.")
                    client.query(f"DROP TABLE {restored_table_ref};").result()
                except NotFound:
                    # This is the expected outcome if the script completed successfully or failed before creating the table.
                    context.log.info("No orphaned temporary table found. Cleanup complete.")
                except Exception as cleanup_e:
                    # Log any unexpected errors during the cleanup process itself.
                    context.log.error(f"An error occurred during cleanup: {cleanup_e}")

    return _revert_asset



@dg.asset(
    name="example_table_failure",
    group_name="revert_table_example",
    kinds={"bigquery"},
)
def example_table_failure(context: dg.AssetExecutionContext):
  """
  This asset represents an external data source.
  It doesn't have any computation logic, but is part of the asset graph.
  """
  # Placeholder for external data fetching logic
  pass

default_revert_time_min = 360
example_dataset_id = "some_dataset"
example_table_id = "some_table"
example_asset_key = dg.AssetKey("example_table_failure")
# example, instatiating one of the revert assets
sample_revert_assets = build_revert_asset(example_asset_key, example_dataset_id, example_table_id, default_revert_time_min)