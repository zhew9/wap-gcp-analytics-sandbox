import yaml
from pathlib import Path
from gcp_analytics_sandbox.ingestion.assets.asset_factory_gcs_load import create_gcs_load_asset_from_config
from gcp_analytics_sandbox.ingestion.assets.config_model.gcs_config_model import GCSLoadAssetConfigList
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition



### loading & converting YAML file to a custom Python object/Pydantic Model
# carrying the config parameters for our asset factory
def load_gcs_config(file_path: str) -> GCSLoadAssetConfigList:
    with open(file_path, "r") as f:
        yaml_config = yaml.safe_load(f)
    return GCSLoadAssetConfigList(**yaml_config)

current_dir = Path(__file__).parent
config_file_path = current_dir / "config_yaml" / "gcs_load_config.yaml"

load_to_gcs_factory_config_list = load_gcs_config(config_file_path)

gcs_load_assets = []

default_partition = default_partition_definition
#creating the Dagsters assets using our asset factory function, with the passed configs
for key, asset_factory_config in load_to_gcs_factory_config_list.gcs_load_asset_configurations.items() :
    # print(f"creating asset for: {key} \nfrom configs: {config_file_path} \n")
    gcs_load_assets.append(create_gcs_load_asset_from_config(asset_factory_config, default_partition))

