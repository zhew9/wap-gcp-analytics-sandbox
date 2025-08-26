from dagster_dbt import DbtCliResource
from pathlib import Path

from gcp_analytics_sandbox.transformation.assets.dbt_project_rep import dbt_project

dbt_bq_analytics_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir=dbt_project.profiles_dir,
)
