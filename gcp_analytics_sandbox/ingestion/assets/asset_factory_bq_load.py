import dagster as dg
from dagster import AssetKey, AssetRecordsFilter
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import timedelta, date
from dagster_gcp import BigQueryResource
from typing import Iterator

# import Pydantic model definitions for configs
from gcp_analytics_sandbox.ingestion.assets.config_model.bq_config_model import SchemaFieldConfig, BQLoadJobConfig, BQAssetFactoryConfig

# currently just using auto-detect schema for this project due to frequency of changing the schema
# but in case anyone wants to specify a schema, you can use this, though for nested fields
# you will may need to test and extend the commented-out section for nested fields
def setup_bq_schema(schema_config: list[SchemaFieldConfig]) -> list[bigquery.SchemaField]:
    schema_list = []
    # setup basic SchemaField parameters for BigQuery, more optional parameters can be found in docs
    for column in schema_config:
        #experimental/untested: if the column has nested/repeated columns
        if column.type == "RECORD":
            raise ValueError("does not support nested columns by default, but you can uncomment the experimental handling of nested columns in the code for the BQ load asset factory")
            # if column.fields:
            #     curr_column_schema = bigquery.SchemaField(
            #         name=column.name,
            #         field_type=column.type,
            #         mode=column.mode,
            #         fields=setup_bq_schema(column.fields),)
            # else:
            #     raise ValueError(f"column: {column.name} is of type: RECORD but has no definition for subfields in 'fields' attribute.")
        else:
            curr_column_schema = bigquery.SchemaField(
                    name=column.name,
                    # field type (STRING, INTEGER, TIMSTAMP, NUMERIC, JSON, etc. )
                    field_type=column.type,
                    # optional, mode = one of NULLABLE, REQUIRED and REPEATED
                    mode=column.mode,
                    # ... other parameters
                )
            # other optional parameters
            if column.type == "NUMERIC" or column.type == "BIGNUMERIC":
                if column.rounding_mode:
                    curr_column_schema.rounding_mode = column.rounding_mode
        if column.description:
            curr_column_schema.description = column.description
        schema_list.append(curr_column_schema)

    return schema_list


# main logic for our asset factory, sets up a BigQuery LoadJob using a config and then executes the job
def load_file_to_bq(
    bq: BigQueryResource,
    uris_list: list[str],
    bq_load_job_config: BQLoadJobConfig,
    context: dg.AssetExecutionContext,
): 
    try:
        # use Dagster wrapper of Google Cloud BigQuery client using our resource definition
        # and expects GOOGLE_APPLICATION_CREDENTIALS to be passed as an env var
        # gets a BigQueryResource object from the dagster_gcp library
        with bq.get_client() as bq_client:
            # will use the same job configs for all filepaths (expecting to be all part of the same table)
            if not bq_load_job_config.schema_autodetect:
                job_config = bigquery.LoadJobConfig(
                    # Specify source format (AVRO, PARQUET, ORC recommended)
                    source_format=bigquery.SourceFormat.PARQUET,
                    # Define schema explicitly for reliability, of type: Optional[Sequence[Union[ SchemaField, Mapping[str, Any] ]]]
                    # in our case, our setup function will return a List[bigquery.SchemaField]
                    schema=setup_bq_schema(bq_load_job_config.table_schema),
                    # Controls behavior if the table exists
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    # Controls behavior if the table *doesn't* exist
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    # ... other parameters            
                )
            else:
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    #autodetect schema, works well with self-describing formats like Parquet
                    autodetect=True,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,         
                )
            
            dataset_ref = bigquery.DatasetReference(bq_client.project, bq_load_job_config.dataset_id)
            dataset_options = bq_load_job_config.dataset_options
            try:
                # Try to get the dataset to check if it exists
                bq_client.get_dataset(dataset_ref)  # Make an API request.
                context.log.info(f"Dataset {bq_client.project}.{bq_load_job_config.dataset_id} exists.")

            except NotFound:
                context.log.info(f"Dataset {bq_client.project}.{bq_load_job_config.dataset_id} not found.")
                # If the dataset does not exist, and option is to create it.
                if dataset_options.create_dataset_if_missing:
                    context.log.info(f"Creating dataset {bq_client.project}.{bq_load_job_config.dataset_id}.")
                    # Construct a full Dataset object to send to the API.
                    # You can also specify location, description, labels etc. here.
                    dataset = bigquery.Dataset(dataset_ref)
                    # e.g.: dataset.location = "US" for a multi-region or a can be more specific like "us-west1" 
                    for field_name, value in dataset_options.model_dump(exclude_unset=True).items():
                        if hasattr(dataset, field_name):
                            setattr(dataset, field_name, value)
                            context.log.info(f"  - For dataset: '{bq_load_job_config.dataset_id}', setting field: '{field_name}' to value: '{value}'")
                        else:
                            context.log.warning(f"'{field_name}' is not a valid property for a BigQuery Dataset. It will be ignored.")
                    # Send the dataset to the API for creation, with an explicit timeout.
                    # Make an API request.
                    bq_client.create_dataset(dataset, timeout=30)  # API request
                    context.log.info(f"Created dataset {bq_client.project}.{bq_load_job_config.dataset_id}")

            # might go back and add more error handling
            except Exception as e:
                context.log.info(f"An error occurred: {e}")            
            
            table_ref = bigquery.TableReference(dataset_ref, bq_load_job_config.table_name)
            #check if table exists in BigQuery
            full_table_str = f"{bq_load_job_config.dataset_id}.{bq_load_job_config.table_name}"
            try:
                bq_client.get_table(table_ref)
                context.log.info(f"Table {full_table_str} exists.")
            except NotFound:
                context.log.info(f"Table {full_table_str} not found.")
                # if it doesn't exist, you may want to set partitioning settings on creation,
                # or you can just inherit the settings from the table's dataset
                if bq_load_job_config.table_time_partitioning:
                    context.log.info("Setting up time partitioning for table creation")
                    table_time_partitioning = bq_load_job_config.table_time_partitioning
                    job_config.time_partitioning = bigquery.TimePartitioning(
                        #if set, the table is partitioned by this field. If not set, the table is partitioned by pseudo column PARTITIONTIME (depending on ingestion setting)
                        field=table_time_partitioning.partition_field,
                        #the field must be a top-level TIMESTAMP, DATETIME, or DATE field. Its mode must be NULLABLE or REQUIRED
                        #time to keep the partition in milliseconds, convert from days
                    )
                    if table_time_partitioning.partition_type.upper() == "MONTHLY":
                        job_config.time_partitioning.type_ = bigquery.TimePartitioningType.MONTH 
                    elif table_time_partitioning.partition_type.upper() == "YEARLY":
                        job_config.time_partitioning.type_ = bigquery.TimePartitioningType.YEAR
                    elif table_time_partitioning.partition_type.upper() == "HOURLY":
                        job_config.time_partitioning.type_ = bigquery.TimePartitioningType.HOUR
                    # if explicit table expiration, otherwise defaults to dataset options
                    if table_time_partitioning.partition_expiration_days:
                        job_config.time_partitioning.expiration_ms= int(timedelta(days=table_time_partitioning.partition_expiration_days).total_seconds() * 1000),
    
            # iterate through list of 'uris_list', in case our incremental query produced
            #  multiple files - we will be using the same config for files bound to same table
            load_job_states = []
            for uri in uris_list:
                # load_table_from_uri(source_uris, destination ...) more parameters at: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_load_table_from_uri
                load_job = bq_client.load_table_from_uri(
                    #URIs of data files to be loaded; in format
                    uri,
                    #Table into which data is to be loaded. If a string is passed in, this method attempts to create a table reference from a string using from_string.
                    table_ref,
                    # provide basic job configuration
                    job_config=job_config,
                    # ... additional optional job parameters
                )
                context.log.info(f"Starting BigQuery load job from '{uri}' into '{bq_load_job_config.table_name}'")
                # start the job and waits for the job to complete
                load_job.result()
                # maybe eventually convert most print statements to context logs
                context.log.info(f"Load job completed: {load_job.job_id}")
                if load_job.errors:
                    context.log.error(f"Load job errors: {load_job.errors}")
                    raise Exception("BigQuery load job failed")
                context.log.info(f"Table {bq_load_job_config.table_name} now has {load_job.output_rows} rows.")
                load_job_states.append(load_job.state)

            return load_job_states 
        #bq_client.close()
    
    except Exception as e:
        raise dg.Failure(f"Error loading filepaths: {uris_list} to BQ: {e}") from e
    

def create_bq_load_asset_from_dict(
        asset_factory_config: BQAssetFactoryConfig,  # Change to accept AssetFactoryConfig
        default_partition_def: dg.DailyPartitionsDefinition = None
) -> dg.AssetsDefinition:
    """Creates a Dagster asset for loading a table to BigQuery from GCS based on the provided configuration."""    
    # get parameters for setting asset metadata from configs
    asset_definition = asset_factory_config.dagster_asset_definition
    bq_load_job_config = asset_factory_config.bq_load_job_config

    partitions_def = None
    if asset_definition.partition_type:
        if default_partition_def:
            partitions_def = default_partition_def
        else:
            #define partition type/frequency, excluding hourly - expecting 'daily' for all examples in this project
            if asset_definition.partition_type == "daily":
                partitions_def = dg.DailyPartitionsDefinition(start_date=asset_definition.partitions_start_date,
                    end_date=asset_definition.partitions_end_date,)
            elif asset_definition.partition_type == "weekly":
                partitions_def = dg.WeeklyPartitionsDefinition(start_date=asset_definition.partitions_start_date,
                    end_date=asset_definition.partitions_end_date,)
            elif asset_definition.partition_type == "monthly":
                partitions_def = dg.MonthlyPartitionsDefinition(start_date=asset_definition.partitions_start_date,
                    end_date=asset_definition.partitions_end_date,)
    
    upstream_asset_key = AssetKey(asset_definition.upstream_input_asset)

    if asset_definition.asset_name:
        asset_name = asset_definition.asset_name
    else:
        asset_name = f"load_to_bq_table_{bq_load_job_config.table_name}"
    
    
    # customizing automation condition, because this project is often for backfills
    # so we want to change the automation condition to trigger for backfills/historic data
    # more docs can be found here: https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/customizing-eager-condition

    # set asset metadata
    @dg.asset(
        name=asset_name,
        deps=asset_definition.asset_dependencies,
        kinds=asset_definition.kinds,
        group_name=asset_definition.group_name,
        partitions_def=partitions_def,
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _load_gcs_to_bq_asset(context: dg.AssetExecutionContext, bigquery: BigQueryResource) -> Iterator[dg.MaterializeResult]:
        """
        Load GCS files into BigQuery as tables, using YAML config files for asset and job config
        """

        context.log.info(f"Running for partition range: {sorted(context.partition_keys)}")

        for partition_key in sorted(context.partition_keys):

            context.log.info(f"Running for partition: {partition_key}")

            # this is one of the methods for passing data between Dagster assets, although only recommended
            # for smaller data or inter-asset communication (like a list of filepaths/uris like in our case)
            # since this persists the data locally ()
            # other cases would be to use external storage (could be managed in the form of a Dagster IO manager)
            # or tempory-pickle/in-memory-like arugments

            # initialize a AssetRecordsFilter, with arguments to filter past materializations of our upstream asset
            records_filter = AssetRecordsFilter(
                asset_key=upstream_asset_key,
                asset_partitions=[partition_key],
            )
            # fetch_materializations() gets the most recent materialization (using limit =1, in descending order by default) 
            # according to the filter conditions, returns a EventRecordsResult: an object which holds a list of EventLogRecords
            # in it's EventRecordsResult.records attribute
            records = context.instance.fetch_materializations(records_filter,limit=1).records
            if not records:
                raise dg.Failure(
                    description=f"No materialization record found for upstream asset '{upstream_asset_key}' on partition '{partition_key}'."
                )
            # gets the first EventLogRecord from the list
            latest_record = records[0]
            # EventLogRecord contains an AssetMaterialization from it's .asset_materialization attribute
            materialization = latest_record.asset_materialization
            # AssetMaterialization is the result our upstream asset's MaterializationResult 
            # from which we can fetch the metadata we passed earlier, holding our list of gcs uris
            gcs_uri_list = materialization.metadata.get("gcs_uri_list").value

            
            if not gcs_uri_list:
                raise dg.Failure(
                    description=f"Missing 'gcs_uri_list' metadata for asset {upstream_asset_key} in partition {partition_key}."
                )
            
            load_job_results = load_file_to_bq(bigquery, gcs_uri_list, bq_load_job_config, context)

            yield dg.MaterializeResult(
                metadata={
                    "partition_key": partition_key,
                    "load_job_results": load_job_results,
                }
            )


    return _load_gcs_to_bq_asset