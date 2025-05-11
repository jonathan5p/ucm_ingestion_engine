import json
import logging
from enum import StrEnum
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType

from ingestion_engine.readers.base_reader import BaseReader

logger = logging.getLogger(__name__)


class ValidReadOptions(StrEnum):
    READ_MODE = "readMode"
    SRC_PATH = "src_path"
    SCHEMA_PATH = "schema"
    USING_AUTOLOADER = "using_autoloader"
    AUTOLOADER_SCHEMA_LOCATION = "cloudFiles.schemaLocation"
    KAFKA_TOPIC = "subscribe"
    KAFKA_STARTING_OFFSETS = "startingOffsets"
    KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
    KAFKA_SECURITY_PROTOCOL = "kafka.security.protocol"
    KAFKA_SASL_MECHANISM = "kafka.sasl.mechanism"
    KAFKA_SASL_JAAS_CONFIG = "kafka.sasl.jaas.config"
    KAFKA_INCLUDE_HEADERS = "includeHeaders"
    KAFKA_VALUE_FORMAT = "value.format"
    KAFKA_VALUE_SCHEMA = "value.schema"


class ValidReadFormats(StrEnum):
    IMG = "image"
    AVRO = "avro"
    PARQUET = "parquet"
    JSON = "json"
    DELTA = "delta"
    KAFKA = "kafka"


class SparkReaderBuilder:
    def __init__(self):
        self._spark = None
        self._format = None
        self._options = {}

    def _set_option(self, key: ValidReadOptions, value: Any):
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
        valid_formats = ValidReadFormats.__members__.values()
        if format not in valid_formats:
            raise ValueError(
                f"Unsupported format: {format}. Supported formats are: {valid_formats}"
            )
        self._format = format
        return self

    def set_read_mode(self, mode: str):
        self._set_option(ValidReadOptions.READ_MODE, mode)
        return self

    def set_source_path(self, source_path: str):
        self._set_option(ValidReadOptions.SRC_PATH, source_path)
        return self

    def set_schema_path(self, schema_path: str):
        with open(schema_path, "r") as f:
            try:
                schema = StructType.fromJson(json.load(f))
                self._set_option(ValidReadOptions.SCHEMA_PATH, schema)
            except ValueError as e:
                raise ValueError(f"Invalid schema file: {schema_path}. Error: {e}")
        return self

    def set_using_autoloader(self, using_autoloader: bool = False):
        self._set_option(ValidReadOptions.USING_AUTOLOADER, using_autoloader)
        return self

    def set_autoloader_schema_location(self, schema_location: str):
        self._set_option(ValidReadOptions.AUTOLOADER_SCHEMA_LOCATION, schema_location)
        return self

    def set_kafka_topic(self, topic: str):
        self._set_option(ValidReadOptions.KAFKA_TOPIC, topic)
        return self

    def set_kafka_starting_offsets(self, starting_offsets: str):
        self._set_option(ValidReadOptions.KAFKA_STARTING_OFFSETS, starting_offsets)
        return self

    def set_kafka_bootstrap_servers(self, bootstrap_servers: str):
        self._set_option(ValidReadOptions.KAFKA_BOOTSTRAP_SERVERS, bootstrap_servers)
        return self

    def set_kafka_security_protocol(self, security_protocol: str):
        self._set_option(ValidReadOptions.KAFKA_SECURITY_PROTOCOL, security_protocol)
        return self

    def set_kafka_include_headers(self, include_headers: str = "true"):
        self._set_option(ValidReadOptions.KAFKA_INCLUDE_HEADERS, include_headers)
        return self

    def set_kafka_sasl_mechanism(self, sasl_mechanism: str):
        self._set_option(ValidReadOptions.KAFKA_SASL_MECHANISM, sasl_mechanism)
        return self

    def set_kafka_sasl_jaas_config(self, jaas_config: str):
        self._set_option(ValidReadOptions.KAFKA_SASL_JAAS_CONFIG, jaas_config)
        return self

    def set_kafka_value_format(self, value_format: str):
        self._set_option(ValidReadOptions.KAFKA_VALUE_FORMAT, value_format)
        return self

    def set_kafka_value_schema(self, value_schema: str):
        self._set_option(ValidReadOptions.KAFKA_VALUE_SCHEMA, value_schema)
        return self

    def build(self):
        if self._spark is None:
            raise ValueError(
                "Spark session must be set before building the spark reader."
            )

        if self._format is None:
            raise ValueError("Format must be set before building the spark reader.")

        if (
            self.options.get(ValidReadOptions.USING_AUTOLOADER, False) is True
            and ValidReadOptions.AUTOLOADER_SCHEMA_LOCATION not in self._options
        ):
            raise ValueError(
                "Schema location must be set before building the spark reader when using Databricks Autoloader."
            )

        required_kafka_options = {
            ValidReadOptions.KAFKA_BOOTSTRAP_SERVERS,
            ValidReadOptions.KAFKA_TOPIC,
            ValidReadOptions.KAFKA_STARTING_OFFSETS,
        }

        missing = required_kafka_options - self._options.keys()
        if len(missing) > 0 and self._format == ValidReadFormats.KAFKA:
            raise ValueError(
                f"Kafka options must be set before building the spark reader for a kafka data source. Missing options: {missing}"
            )

        return SparkReader(self)


class SparkReader(BaseReader):
    def __init__(self, builder: SparkReaderBuilder):
        self.config = builder.options
        self.spark = builder.spark
        self.format = builder.format

    def read(self) -> DataFrame:
        match self.format:
            case ValidReadFormats.PARQUET:
                return self._read_file(ValidReadFormats.PARQUET)
            case ValidReadFormats.JSON:
                return self._read_file(ValidReadFormats.JSON)
            case ValidReadFormats.DELTA:
                return self._read_file(ValidReadFormats.DELTA)
            case ValidReadFormats.KAFKA:
                return self._read_kafka()
            case ValidReadFormats.AVRO:
                return self._read_file(ValidReadFormats.AVRO)
            case ValidReadFormats.IMG:
                return self._read_img()
            case _:
                raise ValueError(f"Unsupported format: {self.format}.")

    def _read_file(self, format: ValidReadFormats) -> DataFrame:
        file_path = self.config.pop(ValidReadOptions.SRC_PATH)
        schema = self.config.pop(ValidReadOptions.SCHEMA_PATH, None)

        if self.config.pop(ValidReadOptions.USING_AUTOLOADER, False) is True:
            df = (
                self.spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", format)
                .option(
                    "cloudFiles.schemaLocation",
                    self.config.pop(ValidReadOptions.AUTOLOADER_SCHEMA_LOCATION),
                )
                .options(**self.config)
            )
        else:
            df = self.spark.readStream.format(format).options(**self.config)

        if schema is not None:
            df = df.schema(schema)

        return df.load(file_path)

    def _read_kafka(self) -> DataFrame:
        df = (
            self.spark.readStream.format(ValidReadFormats.KAFKA)
            .options(**self.config)
            .load()
        )

        match self.config.get(ValidReadOptions.KAFKA_VALUE_FORMAT, "string"):
            case "string":
                df = df.withColumn("value", F.expr("cast(value as string)"))
            case "json":
                df = df.withColumn(
                    "value",
                    F.from_json(
                        F.col("value").cast("string"),
                        self.config.get(ValidReadOptions.KAFKA_VALUE_SCHEMA),
                    ),
                )
            case "avro":
                df = df.withColumn(
                    "value",
                    from_avro(
                        F.expr("substring(value,6,length(value)-5)"),
                        self.config.get(ValidReadOptions.KAFKA_VALUE_SCHEMA),
                    ),
                )

        return df

    def _read_img(self) -> DataFrame:
        img_path = self.config.pop(ValidReadOptions.SRC_PATH)

        df = (
            self.spark.read.format("image")
            .options(**self.config)
            .load(img_path)
            .select(
                F.col("image.origin").alias("src_path"),
                "image.height",
                "image.width",
                F.col("image.nChannels").alias("n_channels"),
                "image.mode",
                F.col("image.data").alias("image_data"),
            )
        )

        return df
