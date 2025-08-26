import yaml
from pathlib import Path
from gcp_analytics_sandbox.ingestion.assets.asset_factory_bq_load import create_bq_load_asset_from_dict
from gcp_analytics_sandbox.ingestion.assets.config_model.bq_config_model import BQAssetFactoryConfigList
from dagster import EnvVar, DailyPartitionsDefinition
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition


### loading & converting YAML file to a custom Python object/Pydantic Model
# carrying the config parameters for our asset factory
def load_bq_config(file_path: str) -> BQAssetFactoryConfigList:
    with open(file_path, "r") as f:
        yaml_config = yaml.safe_load(f)
    return BQAssetFactoryConfigList(**yaml_config)

current_dir = Path(__file__).parent
config_file_path = current_dir / "config_yaml" / "bq_load_config.yaml"

load_to_bq_factory_config_list = load_bq_config(config_file_path)

bq_load_assets = []
default_partition = default_partition_definition
#creating the Dagsters assets using our asset factory function, with the passed configs
for key, asset_factory_config in load_to_bq_factory_config_list.bq_load_asset_configurations.items():
    # print(f"creating asset for: {key} \nfrom configs: {config_file_path} \n")
    bq_load_assets.append(create_bq_load_asset_from_dict(asset_factory_config, default_partition))
