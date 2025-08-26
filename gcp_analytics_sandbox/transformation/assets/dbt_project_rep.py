from pathlib import Path
from dagster_dbt import DbtProject

# If the dbt project folder's path is in an adjacent folder to the parent of this file
dbt_project = DbtProject(
  project_dir=Path(__file__).parent.joinpath("..","dbt_bq_analytics").resolve()
)

# This helper function runs `dbt parse` to generate the manifest.json
# which Dagster reads to create the assets.
dbt_project.prepare_if_dev()