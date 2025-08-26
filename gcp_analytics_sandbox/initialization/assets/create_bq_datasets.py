import dagster as dg
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound

#dagster
@dg.asset(
    name="create_bq_datasets",
    kinds={"gcp", "bigquery"},
    group_name="setup_bigquery",
)
def setup_bq_datasets(context: dg.AssetExecutionContext, bigquery: BigQueryResource) -> dg.MaterializeResult:
    """
    Creates datasets for BigQuery if they do not already exist.

    This asset reads a configuration of datasets to create and applies
    the specified options (e.g., location, description, labels).
    """

    with bigquery.get_client() as bq_client:
        # Configuration for datasets to be created.
        # The key is the dataset name, and the value is a dictionary of its properties.
        # These properties correspond to attributes of the google.cloud.bigquery.Dataset object.
        # more properties: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.dataset.Dataset
        
        # could take a config in the future
        datasets_to_create = {
            "boardgame_retail_raw": {
                "location": "US",
                "description": "dataset to store source, append-only data, in it's raw unaltered form.",
                # 90 days in milliseconds
                # "default_table_expiration_ms": 1000 * 60 * 60 * 24 * 90,
                # "default_rounding_mode": 'ROUND_HALF_AWAY_FROM_ZERO',
                # "max_time_travel_hours": 168, #48-168h,2-7 days
                "labels": {
                    # "env": "dev",
                    "layer": "raw_bronze",
                    # "source": "retail-duckdb",
                    # "domain": "backend-db",
                    # "team": "data-engineering... etc."
                }
            },
            "boardgame_retail_clean": {
                "location": "US",
                "description": "Dataset where raw/source data is deduplicatied before being snapshotted, keeping data as close to source as possible",
                "labels": {
                    # I am using a more layered architecture, but also denoting equivalent medallion layer
                    "layer": "clean_bronze", 
                }
            },
            "boardgame_retail_snapshots": {
                "location": "US",
                "description": "Dataset for storing historical snapshots of mutable source data, SCD2s managed by dbt.",
                "labels": {
                    "layer": "snapshots_bronze",
                }
            },
            "boardgame_retail_staging": {
                "location": "US",
                "description": "Dataset for storing quality-checked, cleaned, and conformed views of data and snapshots. E.g. deduplication of non-snapshot data",
                "labels": {
                    "layer": "staging_silver",
                }
            },
            "boardgame_retail_prep": {
                "location": "US",
                "description": "Dataset for storing the bulk of repeated and compute-heavy transformations in preparation for marts",
                "labels": {
                    "layer": "prep_silver",
                }
            },
            "boardgame_retail_mart_blue": {
                "location": "US",
                "description": "Dataset for holding dimensional models and data marts, either for auditing for WAP or as the live dataset",
                "labels": {
                    "deployment_status": "live",
                    "layer": "audit_gold",
                }
            },
            "boardgame_retail_mart_green": {
                "location": "US",
                "description": "Dataset for holding dimensional models and data marts, either for auditing for WAP or as the live dataset",
                "labels": {
                    "deployment_status": "audit",
                    "layer": "audit_gold",
                }
            },
            "boardgame_retail_live": {
                "location": "US",
                "description": "Dataset of views pointing to the marts and dimensional models, for analysts and other downstream users, usually accompanied by by stricter access control measure",
                "labels": {
                    "layer": "live_gold",
                }
            },
        }
        datasets_created = []

        for dataset_name, dataset_options in datasets_to_create.items():

            dataset_ref = bq.DatasetReference(bq_client.project, dataset_name)

            try:
                # Try to get the dataset to check if it exists
                bq_client.get_dataset(dataset_ref)  # Make an API request.
                context.log.info(f"Dataset {bq_client.project}.{dataset_name} already exists.")

            except NotFound:
                context.log.info(f"Dataset {bq_client.project}.{dataset_name} not found. Creating it.")
                # Construct a full Dataset object to send to the API.
                dataset = bq.Dataset(dataset_ref)

                # Dynamically set attributes on the Dataset object from the options dictionary.
                for key, value in dataset_options.items():
                    if hasattr(dataset, key):
                        setattr(dataset, key, value)
                        context.log.info(f"  - For dataset '{dataset_name}', setting field: '{key}' to value: '{value}'")
                    else:
                        context.log.warning(f"'{key}' is not a valid property for a BigQuery Dataset. It will be ignored.")

                # Send the dataset to the API for creation, with an explicit timeout.
                created_dataset = bq_client.create_dataset(dataset, timeout=30)  # API request

                context.log.info(f"Successfully created dataset {bq_client.project}.{created_dataset.dataset_id}")
                datasets_created.append(created_dataset.dataset_id)

    return dg.MaterializeResult(
        metadata={
            "datasets_created": datasets_created,
            "num_datasets_created": len(datasets_created)
        },
    )