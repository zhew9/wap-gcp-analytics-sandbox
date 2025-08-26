from typing import Optional, Dict, List, Set
from pydantic import BaseModel, field_validator, ValidationInfo
from datetime import datetime

class GCSAssetDefinition(BaseModel):
    partition_type: Optional[str]
    partitions_start_date: Optional[str]
    partitions_end_date: Optional[str]
    day_offset: Optional[int]
    kinds: Optional[Set[str]]
    group_name: Optional[str]
    asset_dependencies: Optional[List[str]]

    # currently few/barebones, but may add more checks and validators later
    @field_validator("partition_type", mode="before")
    @classmethod
    def parse_mode(cls, value):
        if isinstance(value, str):
            value = value.lower()
            accepted = ["daily", "weekly", "monthly"]
            if value in accepted:
                return value
            else:
                raise ValueError(f"Invalid input value for 'partition_type': {value}. Must be one of: {accepted}")
        else:
            raise TypeError(f"mode must be of type: str , but instead got type: {type(value)}")
        
    @field_validator("day_offset")
    @classmethod
    def validate_day_offset(cls, v: Optional[int], info: ValidationInfo) -> Optional[int]:
        """
        Validates day_offset based on the value of partition_type.
        - For 'weekly', day_offset must be between 0 and 7.
        - For 'monthly', day_offset must be between 0 and 31.
        """
        # If day_offset is not provided, no validation is needed.
        if v is None:
            return v

        # Access the already validated data for other fields.
        partition_type = info.data.get('partition_type')

        if partition_type == 'weekly' and not (0 <= v <= 7):
            raise ValueError(f"For 'weekly' partitioning, day_offset must be between 0 and 7. You provided: {v}")

        if partition_type == 'monthly' and not (0 <= v <= 31):
            raise ValueError(f"For 'monthly' partitioning, day_offset must be between 0 and 31. You provided: {v}")

        return v

def is_valid_sql_identifier(value: str) -> bool:
    """
    Checks if a string is a valid, simple SQL identifier.
    - Conforms to Python's identifier rules.
    - Is not a Python keyword (which covers many SQL keywords).
    """
    return value.isidentifier()

class GCSLoadConfig(BaseModel):
    table_name: str
    date_field: str
    columns: Optional[List[str]] = None
    chunk_size: Optional[int] = None
    data_lake_partitioning: Optional[str] = None
    gcs_base_path: str
    compression: Optional[str] = "zstd"
    load_all_cols: Optional[bool] = True

    @field_validator('table_name', 'date_field')
    @classmethod
    def check_simple_identifiers(cls, v: str) -> str:
        """
        Validates that table_name and date_field are valid identifiers.
        """
        if not is_valid_sql_identifier(v):
            # This error message will be captured by Pydantic and shown to the user.
            raise ValueError(f"'{v}' is not a valid identifier. It must contain only letters, numbers, or underscores, and cannot start with a number.")
        return v

    @field_validator('columns')
    @classmethod
    def check_column_list_identifiers(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Validates that every string within the optional 'columns' list is a valid identifier.
        """
        if v is None:
            # If columns is not provided, there's nothing to validate.
            return None
        for column_name in v:
            if not is_valid_sql_identifier(column_name):
                raise ValueError(f"Column name '{column_name}' is not a valid identifier.")
        return v

    @field_validator("data_lake_partitioning", mode="before")
    @classmethod
    def parse_mode(cls, value):
        if value is None:  
            return None
        if isinstance(value, str):
            value = value.lower()
            accepted = ["daily", "weekly", "monthly"]
            if value in accepted:
                return value
            else:
                raise ValueError(f"Invalid input value for 'data_lake_partitioning': {value}. Must be one of: {accepted}")
        else:
            raise TypeError(f"mode must be of type: str or None, but instead got type: {type(value)}")


class GCSLoadAssetConfig(BaseModel):
    asset_definition: GCSAssetDefinition
    gcs_load_config: GCSLoadConfig


class GCSLoadAssetConfigList(BaseModel):
    gcs_load_asset_configurations: Dict[str, GCSLoadAssetConfig]
