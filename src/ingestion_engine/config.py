import os

import yaml
from pydantic import BaseModel


class BaseConfig(BaseModel):
    @classmethod
    def read_config_from_yaml(cls, config_file: str):
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        return cls(**config)


class ReadConfig(BaseModel):
    format: str = "parquet"
    source_path: str | None = None
    read_mode: str = "batch"
    schema_path: str | None = None

    # Databricks autoloader options
    using_autoloader: bool = False
    autoloader_schema_location: str | None = None

    # Kafka Specific options
    topic: str | None = None
    starting_offsets: str | None = None
    kafka_bootstrap_servers: str | None = None
    kafka_security_protocol: str | None = None
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_jaas_config: str | None = os.getenv("KAFKA_SASL_JAAS_CONFIG", None)
    kafka_include_headers: str | None = None


class WriteConfig(BaseModel):
    target_path: str
    catalog_name: str | None = None
    database_name: str | None = None
    table_name: str | None = None
    format: str = "delta"
    write_mode: str = "append"
    partition_by: str | None = None
    merge_schema: bool = False
    etl_mode: str = "batch"
    etl_interval: str | None = None


class TaskConfig(BaseConfig):
    task_name: str
    read_config: ReadConfig
    write_config: WriteConfig
    owner: str = "Jonathan Roncancio"
