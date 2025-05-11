import os
import tempfile
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ingestion_engine.writers.spark_writer import (SparkWriterBuilder,
                                                   ValidWriteOptions)


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.appName("test_app")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.0")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark
    spark.stop()


class TestSparkWriter:
    def convert_df_to_stream(
        self, spark: SparkSession, df: DataFrame, format: str, tmp_dir: str
    ) -> DataFrame:
        target_path = f"{tmp_dir}/_streaming_test_dataset/"
        df.write.mode("overwrite").format(format).save(target_path)

        stream = spark.readStream.format(format).option("path", target_path)

        if format != "delta":
            stream = stream.schema(df.schema)

        return stream.load()

    @pytest.mark.parametrize(
        "format",
        ["parquet", "json", "delta"],
    )
    def test_write_batch(self, format: str, spark):
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_path = os.path.join(tmp_dir, f"target_{format}")
            builder = (
                SparkWriterBuilder()
                .set_spark_session(MagicMock())
                .set_format(format)
                .set_target_path(target_path)
                .set_etl_mode("batch")
                .set_write_mode("append")
            )

            writer = builder.build()

            test_data = [
                {"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 35},
                {"name": "Eve   A.", "age": 28},
            ]

            streaming_df = self.convert_df_to_stream(
                spark, spark.createDataFrame(test_data), format, tmp_dir
            )

            writer.write(streaming_df)

            read_df = spark.read.format(format).load(target_path)
            assert read_df.count() == len(test_data)

    @pytest.mark.parametrize(
        "format",
        ["parquet", "json", "delta"],
    )
    def test_write_stream(self, format: str, spark):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = os.path.join(tmp_dir, "source")
            target_path = os.path.join(tmp_dir, f"target_{format}")

            os.makedirs(source_path, exist_ok=True)

            schema = StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            )

            test_data = [
                {"name": "John D.", "age": 30},
                {"name": "Alice G.", "age": 25},
                {"name": "Bob T.", "age": 35},
                {"name": "Eve A.", "age": 28},
            ]
            df = spark.createDataFrame(test_data, schema)
            df.write.format(format).mode("overwrite").save(source_path)

            streaming_df = spark.readStream.format(format)

            if format != "delta":
                streaming_df = streaming_df.schema(schema)

            streaming_df = streaming_df.load(source_path)

            builder = (
                SparkWriterBuilder()
                .set_spark_session(spark)
                .set_format(format)
                .set_target_path(target_path)
                .set_etl_mode("streaming")
                .set_write_mode("append")
                .set_etl_interval("1 second")
            )

            writer = builder.build()

            query = writer.write(streaming_df, sync_write=False)

            if not query.awaitTermination(10):
                query.stop()

            read_df = spark.read.format(format).load(target_path)
            result = [row.age for row in read_df.orderBy("age").collect()]

            expected = [25, 28, 30, 35]

            assert result == expected

    @pytest.mark.parametrize(
        "format",
        ["parquet", "json", "delta"],
    )
    def test_write_with_partitions(self, format: str, spark):
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_path = os.path.join(tmp_dir, f"target_{format}")
            builder = (
                SparkWriterBuilder()
                .set_spark_session(MagicMock())
                .set_format(format)
                .set_target_path(target_path)
                .set_write_mode("append")
                .set_etl_mode("batch")
                .set_partition_by("age")
            )

            writer = builder.build()

            test_data = [
                {"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 25},
                {"name": "Eve   A.", "age": 28},
            ]

            streaming_df = self.convert_df_to_stream(
                spark, spark.createDataFrame(test_data), format, tmp_dir
            )

            writer.write(streaming_df)

            read_df = spark.read.format(format).load(target_path)
            assert read_df.count() == len(test_data)
            assert read_df.rdd.getNumPartitions() == 4


class TestSparkWriterBuilder:
    def test_set_spark_session(self):
        spark_session = MagicMock()
        writer_builder = SparkWriterBuilder().set_spark_session(spark_session)
        assert writer_builder.spark == spark_session

    def test_set_format(self):
        writer_builder = SparkWriterBuilder().set_format("json")
        assert writer_builder.format == "json"

    def test_set_invalid_format(self):
        writer_builder = SparkWriterBuilder()
        with pytest.raises(ValueError):
            writer_builder.set_format("invalid_format")

    def test_set_target_path(self):
        writer_builder = SparkWriterBuilder().set_target_path("path/to/target")
        assert writer_builder.options[ValidWriteOptions.TARGET_PATH] == "path/to/target"

    def test_set_write_mode(self):
        writer_builder = SparkWriterBuilder().set_write_mode("append")
        assert writer_builder.options[ValidWriteOptions.WRITE_MODE] == "append"

    def test_set_invalid_write_mode(self):
        writer_builder = SparkWriterBuilder()
        with pytest.raises(ValueError):
            writer_builder.set_write_mode("invalid_mode")

    def test_set_partition_by_single_column(self):
        writer_builder = SparkWriterBuilder().set_partition_by("column1")
        assert writer_builder.options[ValidWriteOptions.PARTITION_BY] == ["column1"]

    def test_set_partition_by_multiple_columns(self):
        writer_builder = SparkWriterBuilder().set_partition_by("column1,column2")

        assert writer_builder.options[ValidWriteOptions.PARTITION_BY] == [
            "column1",
            "column2",
        ]

    def test_build_without_spark_session(self):
        writer_builder = SparkWriterBuilder()
        with pytest.raises(ValueError):
            writer_builder.build()

    def test_build_without_format(self):
        writer_builder = SparkWriterBuilder().set_spark_session(MagicMock())
        with pytest.raises(ValueError):
            writer_builder.build()

    def test_build_with_valid_parameters(self):
        spark_session = MagicMock()
        writer_builder = (
            SparkWriterBuilder()
            .set_spark_session(spark_session)
            .set_format("parquet")
            .set_target_path("path/to/target")
            .set_write_mode("append")
            .set_partition_by("column1,column2")
        )
        writer = writer_builder.build()

        assert writer.spark == spark_session
        assert writer.format == "parquet"
        assert writer.config[ValidWriteOptions.TARGET_PATH] == "path/to/target"
        assert writer.config[ValidWriteOptions.WRITE_MODE] == "append"
        assert writer.config[ValidWriteOptions.PARTITION_BY] == ["column1", "column2"]
