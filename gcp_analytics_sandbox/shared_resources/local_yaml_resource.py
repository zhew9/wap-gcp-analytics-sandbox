import yaml
from pathlib import Path
from dagster import ConfigurableResource, AssetExecutionContext
from typing import Optional

class YamlFileVariableManager(ConfigurableResource):
    """
    A simple resource that manages reads from and writes to a local YAML file that
    stores configs, could also be used a simpler alternative to the other custom
    Dagster Resource for handling Blue/Green WAP deployment
    """
    # The filepath is now a Path object
    filepath: str

    def write_vars(self, vars_to_write: dict, context: Optional[AssetExecutionContext] = None):
        """Writes the provided dictionary to the configured YAML file."""
        if context:
            context.log.info(f"Writing to YAML config file: {self.filepath}")

        with open(Path(self.filepath), "r") as f:
            # get the yaml config as a dictionary
            config = dict(yaml.load(f, Loader=yaml.SafeLoader))
            # update the existing dictionary
            config.update(vars_to_write)
            # write the new dictionary back
            with open(Path(self.filepath), "w") as f:
                yaml.dump(config, f, sort_keys=False)

    def get_var(self, var_key: str, context: Optional[AssetExecutionContext] = None) -> dict:
        """Reads the yaml for the variable key and returns the variable value, if it exists, from the YAML file."""
        if context:
            context.log.info(f"Reading from YAML config file: {self.filepath}")
        
        with open(Path(self.filepath), "r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
            var_value = config
            keys = var_key.split('.')
            for k in keys:
                try:
                    # Try to access the next level in the dictionary, if multi-level YAML
                    var_value = var_value[k]
                except (KeyError, TypeError):
                    # Key not found or current var_value is not a dict
                    context.log.info(f"Key: {var_key} not found or {var_value} is not a dict")
                    return None
                    
            return var_value
