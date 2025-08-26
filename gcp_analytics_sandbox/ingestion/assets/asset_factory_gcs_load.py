import dagster as dg
import pyarrow.parquet as pq
import gcsfs
import datetime
from typing import Iterator
from duckdb import Error as DuckDBError
from dateutil.relativedelta import relativedelta
from dagster_duckdb import DuckDBResource
from math import ceil
from dagster_gcp import GCSResource
from gcp_analytics_sandbox.ingestion.assets.config_model.gcs_config_model import GCSLoadAssetConfig, GCSLoadConfig

# a function for loading a given DuckDB table to some GCS bucket/blob as parquet file according to our data lake's
#  organization and partitioning scheme, separated because it is repeated task by our assets
def load_table_to_gcs(
    read_duckdb: DuckDBResource,
    context: dg.AssetExecutionContext,
    gcs_load_config: GCSLoadConfig,
    partition_type: str,
) -> list[str]:
    """Incrementally queries and loads a table from DuckDB to GCS in Parquet format in daily partitions."""
    try:
        #get partition date and/or other data/resources from the asset's context
        partition_key = context.partition_key
        # calculate start and (exclusive) end date/time bounds for the query
        # DuckDB handles date/datetime objects correctly as parameters
        if partition_type == "hourly":
            # Hourly keys are 'YYYY-MM-DD-HH:MM'
            start_bound = datetime.datetime.strptime(partition_key, "%Y-%m-%d-%H:%M")
            # The upper bound is the beginning of the next hour
            end_bound_exclusive = start_bound + datetime.timedelta(hours=1)
            
        # Daily, Weekly, or Monthly logic, These keys are 'YYYY-MM-DD'
        else: 
            base_date = datetime.datetime.strptime(partition_key, "%Y-%m-%d").date()
            if partition_type == "daily":
                start_bound = base_date
                end_bound_exclusive = start_bound + datetime.timedelta(days=1)
            elif partition_type == "weekly":
                start_bound = base_date - datetime.timedelta(days=base_date.weekday())
                end_bound_exclusive = start_bound + datetime.timedelta(days=7)
            elif partition_type == "monthly":
                start_bound = base_date.replace(day=1)
                end_bound_exclusive = start_bound + relativedelta(months=1)
            else:
                raise ValueError(f"Unsupported partition_type: '{partition_type}'")
        context.log.info(
            f"Processing '{partition_type}' partition for key '{partition_key}'. "
            f"Querying from '{start_bound}' up to (but not including) '{end_bound_exclusive}'."
        )

        #setup our query for querying our tables
        if not gcs_load_config.load_all_cols and gcs_load_config.columns:
            validated_columns = [f'"{col}"' for col in gcs_load_config.columns if col.isidentifier()]
            select_clause = ", ".join(validated_columns)
        else:
            select_clause = "*"
        query = f"""
                SELECT
                    {select_clause}
                FROM
                    "{gcs_load_config.table_name}"
                WHERE
                    "{gcs_load_config.date_field}" >= ?
                    AND "{gcs_load_config.date_field}" < ?
            """
        # connect to duckdb (with a read only connection, for concurrency) and execute our querries
        with read_duckdb.get_connection() as conn:
            context.log.info(f"Executing query: {query}")
            # Execute the query and fetch the result as a record batch reader - which is a streaming reader and uses a 
            # server-side cursor, so we can process the data in chunks if query results are too large for memory
            # fetch_record_batch() takes an optional int chunk_size argument for how many rows are in a single batch
            default_chunk_size = gcs_load_config.chunk_size or 32768
            try:
                #first get the count, mainly for logging progression and determining filenames for potential batches
                count_query = f'SELECT COUNT(*) FROM "{gcs_load_config.table_name}" WHERE "{gcs_load_config.date_field}" >= ? AND "{gcs_load_config.date_field}" < ?'
                count_result = conn.execute(count_query, [start_bound, end_bound_exclusive]).fetchone()[0]
                context.log.info(f"Found {count_result} rows in the date range between {start_bound} and {end_bound_exclusive} (exclusive)")
                print(f"Found {count_result} rows in the date range.")
                # query and fetch the rows from DuckDB in a stream (broken up into chunk-sized batches to fit into memory)
                record_batch_reader = conn.execute(
                    query, [start_bound, end_bound_exclusive]
                ).fetch_record_batch(rows_per_batch=(gcs_load_config.chunk_size or default_chunk_size))
                context.log.info(f"successfully fetched {count_result} updated rows from {gcs_load_config.table_name} from DuckDB for the date range between {start_bound} and {end_bound_exclusive} (exclusive)")
            except DuckDBError as e:
                context.log.error(f"Error executing query: {e}")
                print(f"error executing DuckDB query: {e}")
            # define our GCS file path according to our datalake organization and partitioning scheme (e.g. date)
            bucket = dg.EnvVar('GCS_INGESTION_BUCKET').get_value()
            # Use the start_bound to create a consistent partitioned path
            path_with_partition = f"{bucket}/{gcs_load_config.gcs_base_path}/year={start_bound.year}"
            # Determine the partitioning scheme for the data lake
            data_lake_partitioning = gcs_load_config.data_lake_partitioning or partition_type
            # partitioning: year=YYYY/week=WW
            if data_lake_partitioning == "weekly":
                path_with_partition += f"/week={start_bound.isocalendar()[1]:02}"
            else:
                # partitioning: year=YYYY/month=MM/day=DD
                path_with_partition += f"/month={start_bound.month:02}"
                if data_lake_partitioning in ["daily", "hourly"]:
                    path_with_partition += f"/day={start_bound.day:02}"
            # store a list of filepaths of the files finished uploading, which we will return and 
            # pass to downstream assets, which load our updated GCS files to BQ
            gcs_uri_list = []
            # setup the GCS filepath
            file_path = f"gs://{path_with_partition}/{gcs_load_config.table_name}"
            if data_lake_partitioning in ["weekly", "monthly"]:
                file_path += f"_{start_bound.strftime("%m%d")}"
            if partition_type == "hourly":
                file_path += f"_{start_bound.strftime('%H')}"

            batch_suffix = ""
            batch_index = 1
            num_batches = ceil(count_result/default_chunk_size)
            # Start reading from the first batch and load each batch as a parquet file to GCS, until all batches are processed
            # read one PyArrow RecordBatch at a time
            if count_result > default_chunk_size:
                context.log.info(f"Splitting query results into {num_batches} batches: {count_result} rows exceeds chunk size of {default_chunk_size}.")
            for batch in record_batch_reader:
                if count_result > default_chunk_size:
                    batch_suffix = f"_part{batch_index:03}"
                context.log.info(f"writing batch: {batch_index} of {num_batches}")
                full_gcs_uri = f"{file_path}{batch_suffix}.parquet"
                # upload to GCS as a file/file part, using PyArrow and GCSFS
                
                # the ParquetWriter will attempt to authenticate using our env vars: 
                # GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT
                # even if we don't instatiate gcsfs or a Dagster GCSResource, PyArrow will create a 
                # gcsfs if the filepath starts with 'gs://' with our env variables
                try:
                    with pq.ParquetWriter(where=full_gcs_uri, schema=batch.schema, compression=gcs_load_config.compression) as writer:
                        writer.write_batch(batch)
                        context.log.info(f"Wrote {batch.num_rows} rows from table: {gcs_load_config.table_name} in batch: {batch_index} to: {full_gcs_uri}")
                except Exception as e:
                    raise dg.Failure(
                        description=f"Failed to write batch {batch_index} to: {full_gcs_uri} - with error: {e}",
                        metadata={
                            "partition_key": context.partition_key,
                            "error_message": str(e),
                            "error_type": str(type(e).__name__),
                            "batch_index": batch_index,
                            "num_batches": num_batches,
                            "gcs_uri": full_gcs_uri,
                            "table_name": gcs_load_config.table_name,
                            }
                        ) from e
                gcs_uri_list.append(full_gcs_uri)
                batch_index += 1

            return gcs_uri_list

    except ValueError as e:
        raise dg.Failure(
            description=f"Failed to parse partition key: {context.partition_key}",
            metadata={"partition_key": context.partition_key, "error": str(e)},
        ) from e
    except DuckDBError as e:
        raise dg.Failure(
            description=f"A DuckDB error occurred while querying table '{gcs_load_config.table_name}'.",
            metadata={
                "table_name": gcs_load_config.table_name,
                "partition_key": context.partition_key,
                "start_bound": str(start_bound),
                "end_bound_exclusive": str(end_bound_exclusive),
                "error": str(e),
            },
        ) from e
    except Exception as e:
        # General catch-all for GCS auth/write errors, env var errors, etc.
        raise dg.Failure(
            description=f"An unexpected error occurred: {e}",
            metadata={
                "partition_key": context.partition_key,
                "last_attempted_gcs_uri": full_gcs_uri, # Will be None if error was before loop
                "table_name": gcs_load_config.table_name,
                "error_type": str(type(e).__name__),
                "error": str(e),
            },
        ) from e


# since we will be using many assets that perform the same thing, we're going to make a Dagster asset factory:
# a function that creates similar/repetitive assets, typically according to config & schema
# (in this example, our config is a Python object parsed from a YAML file but could be JSON, etc.)
def create_gcs_load_asset_from_config(
    gcs_load_asset_config: GCSLoadAssetConfig,
    default_partition_def: dg.PartitionsDefinition = None,
) -> dg.AssetsDefinition:
    """Creates a Dagster asset for loading a table to GCS from a provided
        DuckDB database based on the provided configuration."""    
    asset_definition = gcs_load_asset_config.asset_definition
    
    partitions_def = None
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
    
    # set asset metadata from passed config
    @dg.asset(
        name=f"load_{gcs_load_asset_config.gcs_load_config.table_name}_to_gcs",
        deps=asset_definition.asset_dependencies,
        kinds=asset_definition.kinds,
        group_name=asset_definition.group_name,
        partitions_def=partitions_def,
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _load_table_asset(context: dg.AssetExecutionContext, read_duckdb: DuckDBResource, gcs: GCSResource) -> Iterator[dg.MaterializeResult]:
        """
            Incrementally query our local DuckDB OLTP table and load incremental table results to GCS 
            as a Parquet file or multiple Parquet files* using GCSFS, using YAML config files for asset and job config
            
            *If the query results don't fit in memory, will be broken down into multiple files using a serverside cursor
        """

        context.log.info(f"Running for partition range: {sorted(context.partition_keys)}")

        for partition_key in sorted(context.partition_keys):

            gcs_uri_list = load_table_to_gcs(
                read_duckdb,
                context,
                gcs_load_asset_config.gcs_load_config,
                asset_definition.partition_type,
            )
            # yield a MaterializeResult and pass the list of gcs filepaths/uris 
            # as a Dagster metadata value
            yield dg.MaterializeResult(
                metadata={
                    "partition_key": partition_key,
                    "gcs_uri_list": gcs_uri_list,
                    "num_files": len(gcs_uri_list)
                }
            )

    return _load_table_asset