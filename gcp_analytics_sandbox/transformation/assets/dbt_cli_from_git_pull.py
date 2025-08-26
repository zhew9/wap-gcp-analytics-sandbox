import os
import dagster as dg
from subprocess import Popen
from dagster_dbt import DbtCliResource

# Optional, not used or tested atm. Ideally you use some CI/CD system to ensure Dagster is running
# with your up-to-date dbt project

class DbtGitRepoResource(dg.ConfigurableResource):
    """
    This resource ensures the dbt repository is up-to-date by running `git pull`.
    Git's `pull` command is efficient and will do nothing if the repo is already current.
    """
    git_url: str
    git_branch: str
    repo_local_path: str
    
    def get_dbt_cli_resource(self) -> DbtCliResource:
        """Clones the repo if it doesn't exist, or pulls the latest changes if it does."""
        if os.path.exists(self.repo_local_path):
            # Efficiently pulls the latest changes. Does minimal work if no changes exist.
            print("Repo exists. Pulling latest changes...")
            git_pull = Popen(["git", "-C", self.repo_local_path, "pull", "origin", self.git_branch])
            git_pull.wait()
        else:
            # Clones the repo if it's the first run.
            print("Cloning repo for the first time...")
            git_clone = Popen(["git", "clone", "--branch", self.git_branch, self.git_url, self.repo_local_path])
            git_clone.wait()

        return DbtCliResource(project_dir=self.repo_local_path)
    
""" 
for definitions:

    resources={
            "dbt_repo": DbtGitRepoResource(
                git_url="git@github.com:your-org/your-dbt-repo.git", # Change to your repo URL
                git_branch="main",
                repo_local_path=DBT_PROJECT_LOCAL_PATH,
            )

for assets that use the resource:

    @asset
    def fresh_dbt_models(context, dbt_repo: DbtGitRepoResource):

        # Get the dbt cli resource which points to the correct directory after git pull/clone
        dbt_cli = dbt_repo.get_dbt_cli_resource()

        # Run your dbt commands
        yield from dbt_cli.cli(["build"], context=context).stream()
    
"""