from __future__ import annotations
from pydantic import BaseModel, field_validator
from typing import Dict, Optional, List, Union, Set

#Pydantic models for type conversion, checking, and initialization of values from
#input YAML config files for use in Dagster asset factories
class SchemaFieldConfig(BaseModel):
    name: str
    type: str = "STRING"
    mode: str = "NULLABLE"
    description: Optional[str] = None
    rounding_mode: Optional[str] = None
    #experimental, for fields of type RECORD with nested columns
    fields: Optional[Union[List[SchemaFieldConfig], List[Dict]]] = None

    # currently few/barebones, but may add more checks and validators later
    @field_validator("mode", mode="before")
    def parse_mode(cls, value):
        if isinstance(value, str):
            value = value.upper()
            accepted = ["NULLABLE", "REQUIRED", "REPEATED"]
            if value in accepted:
                return value
            else:
                raise ValueError(f"Invalid input value for mode: {value}. Must be one of {accepted}")
        else:
            raise TypeError(f"mode must be of type: str , but instead got type: {type(value)}")


class TimePartitionConfig(BaseModel):
    partition_field: Optional[str] = None
    partition_type: Optional[str] = None
    partition_expiration_days: Optional[int] = None 

    @field_validator("partition_expiration_days", mode="before")
    def convert_to_ms(cls, value):
        if value is None:  
            return None
        if isinstance(value, int):
            if value <= 0 or value > 365:
                raise ValueError("partition_expiration_days must be a positive integer between 1 and 365.")
            return value
        else:
            raise TypeError(f"partition_expiration_days must be of type: integer , but instead got type: {type(value)}")
        
    @field_validator("partition_type", mode="before")
    def parse_time_partition_type(cls, value):
        if value is None:  
            return None
        if isinstance(value, str):
            value = value.upper()
            if value in ["DAY", "HOUR", "MONTH", "YEAR"]:
                return value
            else:
                raise ValueError(f"Invalid input value for partition_type: {value}. Must be one of 'DAY', 'HOUR', 'MONTH', 'YEAR'.")
        else:
            raise TypeError(f"partition_type must be of type: (string), but instead got type: {type(value)}")


#keeping config checks mostly barebones atm
class BQLoadAssetDefinition(BaseModel):
    asset_name: Optional[str]
    partition_type: Optional[str]
    partitions_start_date: Optional[str]
    partitions_end_date: Optional[str]
    date_field: str
    kinds: Optional[Set[str]]
    group_name: Optional[str]
    upstream_input_asset: str
    asset_dependencies: List[str]

    # currently few/barebones, but may add more checks and validators later
    @field_validator("partition_type", mode="before")
    def parse_mode(cls, value):
        if value is None:  
            return None
        elif isinstance(value, str):
            value = value.lower()
            accepted = ["daily", "weekly", "monthly"]
            if value in accepted:
                return value
            else:
                raise ValueError(f"Invalid input value for 'partition_type': {value}. Must be one of: {accepted}")
        else:
            raise TypeError(f"mode must be of type: str , but instead got type: {type(value)}")

class BQDatasetOptions(BaseModel):
    create_dataset_if_missing: Optional[bool] = False
    location: Optional[str] = "US"
    description: Optional[str]
    labels: Optional[Dict[str, str]]
    default_partition_expiration_ms: Optional[int]
    default_partition_expiration_days: Optional[int]
    default_table_expiration_ms: Optional[int]
    default_table_expiration_days: Optional[int]

class BQLoadJobConfig(BaseModel):
    dataset_id: str
    dataset_options: Optional[BQDatasetOptions]
    table_name: str
    table_schema: Optional[List[SchemaFieldConfig]]
    table_time_partitioning: Optional[TimePartitionConfig] = None
    schema_autodetect: bool = True
    clustering_fields: Optional[List[str]] = None

class BQAssetFactoryConfig(BaseModel):
    dagster_asset_definition: BQLoadAssetDefinition
    bq_load_job_config: BQLoadJobConfig

class BQAssetFactoryConfigList(BaseModel):
    bq_load_asset_configurations: Dict[str, BQAssetFactoryConfig]