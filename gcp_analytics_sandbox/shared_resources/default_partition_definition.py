from dagster import EnvVar, DailyPartitionsDefinition

if EnvVar('DEFAULT_PARTITION_START').get_value():
    default_partition_definition = DailyPartitionsDefinition(
        start_date=EnvVar('DEFAULT_PARTITION_START').get_value(),
        end_date=EnvVar('DEFAULT_PARTITION_END').get_value(),
        timezone=EnvVar('DEFAULT_PARTITION_TIMEZONE').get_value()
    )
else:
    default_partition_definition = None