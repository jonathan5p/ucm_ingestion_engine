import logging
from enum import StrEnum
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from ingestion_engine.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ValidWriteModes(StrEnum):
    APPEND = "append"
    UPDATE = "update"
    COMPLETE = "complete"


class ValidWriteOptions(StrEnum):
    WRITE_MODE = "write_mode"
    FORMAT = "format"
    TARGET_PATH = "path"
    CATALOG_NAME = "catalog_name"
    DATABASE_NAME = "database_name"
    TABLE_NAME = "table_name"
    PARTITION_BY = "partitionBy"
    MERGE_SCHEMA = "mergeSchema"
    ETL_MODE = "etl_mode"
    ETL_INTERVAL = "etl_interval"


class ValidWriteFormats(StrEnum):
    PARQUET = "parquet"
    JSON = "json"
    DELTA = "delta"


class ETLMode(StrEnum):
    BATCH = "batch"
    STREAMING = "streaming"


class SparkWriterBuilder:
    def __init__(self):
        self._spark = None
        self._format = None
        self._options = {}

    def _set_option(self, key: ValidWriteOptions, value: Any):
        self._options[key] = value
        return self

    @property
    def spark(self):
        return self._spark

    @property
    def format(self):
        return self._format

    @property
    def options(self):
        return self._options

    def set_spark_session(self, spark_session: SparkSession):
        self._spark = spark_session
        return self

    def set_format(self, format: str):
        valid_formats = ValidWriteFormats.__members__.values()
        if format not in valid_formats:
            raise ValueError(
                f"Unsupported format: {format}. Supported formats are: {valid_formats}"
            )
        self._format = format
        return self

    def set_target_path(self, path: str):
        self._set_option(ValidWriteOptions.TARGET_PATH, path)
        return self

    def set_catalog_name(self, catalog_name: str):
        self._set_option(ValidWriteOptions.CATALOG_NAME, catalog_name)
        return self

    def set_database_name(self, database_name: str):
        self._set_option(ValidWriteOptions.DATABASE_NAME, database_name)
        return self

    def set_table_name(self, table_name: str):
        self._set_option(ValidWriteOptions.TABLE_NAME, table_name)
        return self

    def set_write_mode(self, mode: str):
        valid_modes = ValidWriteModes.__members__.values()
        if mode not in valid_modes:
            raise ValueError(
                f"Unsupported write mode: {mode}. Supported modes are: {valid_modes}"
            )
        self._set_option(ValidWriteOptions.WRITE_MODE, mode)
        return self

    def set_merge_schema(self, merge_schema: str = "false"):
        self._set_option(ValidWriteOptions.MERGE_SCHEMA, merge_schema)
        return self

    def set_partition_by(self, columns: str):
        self._set_option(ValidWriteOptions.PARTITION_BY, columns.split(","))
        return self

    def set_etl_mode(self, etl_mode: str):
        if etl_mode not in ETLMode.__members__.values():
            raise ValueError(
                f"Unsupported ETL mode: {etl_mode}. Supported modes are: {ETLMode.__members__.values()}"
            )
        self._set_option(ValidWriteOptions.ETL_MODE, etl_mode)
        return self

    def set_etl_interval(self, etl_interval: str):
        """
        Sets the ETL interval for streaming writes.
        Args:
            etl_interval (str): a time interval as a string, e.g. '5 seconds', '1 minute'.

        Returns:
            self: The SparkWriterBuilder instance.
        """

        self._set_option(ValidWriteOptions.ETL_INTERVAL, etl_interval)
        return self

    def build(self):
        if self._spark is None:
            raise ValueError(
                "Spark session must be set before building the spark writer."
            )

        if self._format is None:
            raise ValueError("Format must be set before building the spark writer.")

        if ValidWriteOptions.TARGET_PATH not in self._options:
            raise ValueError(
                "Target path must be set before building the spark writer."
            )

        if self.options.get(
            ValidWriteOptions.ETL_MODE
        ) == ETLMode.STREAMING and not self.options.get(ValidWriteOptions.ETL_INTERVAL):
            raise ValueError("ETL interval must be set for streaming mode.")

        return SparkWriter(self)


class SparkWriter(BaseWriter):
    def __init__(self, builder: SparkWriterBuilder):
        self.config: dict[str, Any] = builder.options
        self.spark = builder.spark
        self.format = builder.format

    def add_metadata(
        self, df: DataFrame, metadata: dict[str, Any] | None = None
    ) -> DataFrame:
        """
        Adds metadata to the DataFrame as additional columns.
        Args:
            df (DataFrame): The DataFrame to which metadata will be added.
            metadata (dict): A dictionary containing metadata key-value pairs.
                             the keys should be strings and the values can be either
                             pyspark.sql.Column objects or values stored in python native types.
        Returns:
            DataFrame: The DataFrame with added metadata columns.
        """

        user_metadata_cols = {}

        baseline_metadata = {
            "_ingestion_time": F.current_timestamp().alias("_ingestion_time"),
            "_ingestion_source": F.input_file_name().alias("_ingestion_source"),
        }

        if metadata is not None:
            for key, value in metadata.items():
                if isinstance(value, Column):
                    user_metadata_cols[key] = value.alias(key)
                else:
                    user_metadata_cols[key] = F.lit(value).alias(key)

        metadata_cols = baseline_metadata | user_metadata_cols

        return df.select(*metadata_cols.values(), "*")

    def write(self, df: DataFrame, sync_write: bool = True):
        write_task = self._write_file(df, self.format)
        if sync_write is True:
            write_task.awaitTermination()
        return write_task

    def _write_file(self, df: DataFrame, format: ValidWriteFormats) -> StreamingQuery:
        target_path = self.config.pop(ValidWriteOptions.TARGET_PATH)
        catalog_name = self.config.pop(ValidWriteOptions.CATALOG_NAME, None)
        database_name = self.config.pop(ValidWriteOptions.DATABASE_NAME, None)
        table_name = self.config.pop(ValidWriteOptions.TABLE_NAME, None)

        writer = (
            df.writeStream.format(format)
            .outputMode(self.config.pop(ValidWriteOptions.WRITE_MODE))
            .option(
                "mergeSchema", self.config.pop(ValidWriteOptions.MERGE_SCHEMA, "false")
            )
            .option("path", target_path)
            .option("checkpointLocation", target_path + "/_checkpoints")
        )

        if self.config.pop(ValidWriteOptions.ETL_MODE) == ETLMode.BATCH:
            writer = writer.trigger(availableNow=True)
        else:
            etl_interval = self.config.pop(ValidWriteOptions.ETL_INTERVAL)
            writer = writer.trigger(processingTime=etl_interval)

        if self.config.get(ValidWriteOptions.PARTITION_BY, None):
            writer = writer.partitionBy(
                *self.config.pop(ValidWriteOptions.PARTITION_BY)
            )

        if catalog_name and database_name and table_name:
            task = writer.toTable(f"{catalog_name}.{database_name}.{table_name}")
        else:
            task = writer.start()

        return task
