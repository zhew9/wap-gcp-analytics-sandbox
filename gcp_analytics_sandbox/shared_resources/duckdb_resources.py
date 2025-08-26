from dagster_duckdb import DuckDBResource
from dagster import EnvVar

read_local_duckdb_resource = DuckDBResource(
    database=EnvVar("LOCAL_DUCKDB"),
    read_only=True
)

write_local_duckdb_resource = DuckDBResource(
    database=EnvVar("LOCAL_DUCKDB")
)