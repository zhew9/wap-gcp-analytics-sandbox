from dagster_duckdb import DuckDBResource
from pathlib import Path
import dagster as dg
import duckdb as db

import gcp_analytics_sandbox.initialization.assets.constants as constants

# helper function to load table schemas, create and populate DuckDB tables from a list of table name
# currently just local print statements but may change to dagster logging later
def load_tables_schema_from_list(table_list: list[str], duckdb: DuckDBResource, parquet_folder: Path, context: dg.AssetExecutionContext):
    
    with duckdb.get_connection() as conn:
        for table_name in table_list:
            try:
                constant_name = f"{table_name.upper()}_SCHEMA"
                # Use getattr to safely get the constant from the 'schemas' module.
                # The third argument to getattr is a default value if the constant is not found.
                table_schema = getattr(constants, constant_name, None)      
                parquet_file = parquet_folder.joinpath(f"{table_name}.parquet")
                conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" {table_schema};')
                context.log.info(f"Table '{table_name}' exists in DuckDB or was created successfully.")

                count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                if count > 0:
                    context.log.info(f"Table '{table_name}' is already populated with {count} rows.")
                    continue
                else:
                    conn.execute(f'INSERT INTO "{table_name}" BY NAME SELECT * FROM "{parquet_file}"')
                    context.log.info(f"Loaded table '{table_name}' from '{parquet_file}'...")

            except db.Error as e:
                # Failure Point 1: DuckDB specific error (SQL syntax, type mismatch, etc.)
                raise dg.Failure(
                    description=f"A DuckDB error occurred while loading table: '{table_name}'.",
                    metadata={
                        "failed_table": table_name,
                        "parquet_file": str(parquet_file),
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }
                ) from e
            except Exception as e:
                # Failure Point 2: Other errors (e.g., FileNotFoundError)
                raise dg.Failure(
                    description=f"An unexpected error occurred while processing table: '{table_name}'.",
                    metadata={
                        "failed_table": table_name,
                        "constant_name": constant_name,
                        "table_schema": table_schema,
                        "parquet_file": str(parquet_file),
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }
                ) from e
    return

@dg.asset(
    name="load_pregenerated_catalog",
    kinds={"python","duckdb"},
    group_name="static_OLTP_initialization",
    pool="write_duckdb",
    deps=["create_bq_datasets"],
)
def load_pregenerated_catalog(context: dg.AssetExecutionContext, write_duckdb: DuckDBResource,) -> dg.MaterializeResult:
    """
    Populate OLTP tables in DuckDB by loading in pregenerated* parquet tables for our 
    retail catalog tables, supporting transaction tables, and bridge tables: 
    
    catalog tables: [products, categories, publishers, suppliers, authors]

    supporting transaction tables: [payment_types, (transaction) channels, promotions (discounts)]

    bridge tables: [categories_products, publishers_products, authors_products, promotion_groups_products]  

    *Generation for the pregenerated parquet tables is included in the data/ folder, as a Jupyter notebook (generate_retail_catalog.ipynb)
    """

    # need to load in order due foreign key restraints, requiring some tables first
    no_foreign_keys = ["authors","categories","suppliers","channels","payment_types","publishers"]
    foreign_keys = ["promotion_groups","products","promotions","promotion_groups_products", "authors_products","categories_products","publishers_products", "suppliers_products"]

    # get the corresponding DuckDB schemas (which are defined as constants in our constants.py file)
    parquet_folder = Path(__file__).parent.joinpath("..","data","mock_data")
    load_tables_schema_from_list(no_foreign_keys, write_duckdb, parquet_folder, context)
    load_tables_schema_from_list(foreign_keys, write_duckdb, parquet_folder, context)
    context.log.info(f"Successfully inserted tables to DuckDB from parquet files")
    
    return dg.MaterializeResult(
         metadata={
            "tables_loaded": no_foreign_keys + foreign_keys,
            "output_folder": parquet_folder,
        }
    )