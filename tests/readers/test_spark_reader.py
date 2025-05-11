import os
import tempfile
from unittest.mock import MagicMock

import numpy as np
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from ingestion_engine.readers.spark_reader import SparkReaderBuilder


@pytest.fixture(scope="module")
def spark():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5",
        "org.apache.spark:spark-avro_2.12:3.5.5",
        "org.apache.kafka:kafka-clients:3.5.1",
        "org.apache.commons:commons-pool2:2.12.1",
    ]

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("test_app")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=packages
    ).getOrCreate()

    yield spark
    spark.stop()


class TestSparkReader:
    @pytest.mark.parametrize(
        "format",
        ["parquet", "json", "delta", "avro"],
    )
    def test_read_file(self, format: str, spark):
        with tempfile.TemporaryDirectory() as tmp_dir:
            src_path = os.path.join(tmp_dir, f"src_{format}")
            test_data = [
                {"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 35},
                {"name": "Eve   A.", "age": 28},
            ]

            test_df = spark.createDataFrame(test_data)
            test_df.write.format(format).save(src_path)

            # Write test_df schema in the tmp_dir
            schema_file = os.path.join(tmp_dir, "schema.json")
            with open(schema_file, "wb") as f:
                f.write(test_df.schema.json().encode())

            builder = (
                SparkReaderBuilder()
                .set_spark_session(spark)
                .set_format(format)
                .set_source_path(src_path)
                .set_read_mode("batch")
            )

            if format != "delta":
                builder = builder.set_schema_path(schema_file)

            reader = builder.build()

            target_path = os.path.join(tmp_dir, f"target_{format}")

            df = (
                reader.read()
                .writeStream.format("parquet")
                .trigger(availableNow=True)
                .outputMode("append")
                .option("schema", test_df.schema)
                .option("path", target_path)
                .option("checkpointLocation", target_path + "/_checkpoints")
                .start()
                .awaitTermination()
            )

            df = spark.read.format("parquet").load(target_path)

            result = [
                row.age
                for row in df.select("age").orderBy("age", ascending=False).collect()
            ]
            expected = [35, 30, 28, 25]

            assert result == expected

    def test_read_img(self, spark):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Generate sample image data
            img1_data = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
            img2_data = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)

            img1_path = os.path.join(tmp_dir, "image1.png")
            img2_path = os.path.join(tmp_dir, "image2.png")

            # Write the image data to files
            with open(img1_path, "wb") as f:
                f.write(img1_data.tobytes())
            with open(img2_path, "wb") as f:
                f.write(img2_data.tobytes())

            builder = (
                SparkReaderBuilder()
                .set_spark_session(spark)
                .set_source_path(str(tmp_dir))
                .set_read_mode("batch")
                .set_format("image")
            )

            reader = builder.build()
            df = reader.read()

            assert df.count() == 2
            assert df.columns == [
                "src_path",
                "height",
                "width",
                "n_channels",
                "mode",
                "image_data",
            ]


class TestSparkReaderBuilder:
    def test_set_spark_session(self):
        spark_session = MagicMock()
        reader_builder = SparkReaderBuilder().set_spark_session(spark_session)
        assert reader_builder.spark == spark_session

    def test_set_format(self):
        reader_builder = SparkReaderBuilder().set_format("json")
        assert reader_builder.format == "json"

    def test_set_invalid_format(self):
        reader_builder = SparkReaderBuilder()
        with pytest.raises(ValueError):
            reader_builder.set_format("invalid_format")

    def test_set_source_path(self):
        reader_builder = SparkReaderBuilder().set_source_path("path/to/source")
        assert reader_builder.options["src_path"] == "path/to/source"

    def test_set_read_mode(self):
        reader_builder = SparkReaderBuilder().set_read_mode("overwrite")
        assert reader_builder.options["readMode"] == "overwrite"

    def test_build_without_spark_session(self):
        reader_builder = SparkReaderBuilder()
        with pytest.raises(ValueError):
            reader_builder.build()

    def test_build_without_format(self):
        reader_builder = SparkReaderBuilder().set_spark_session(MagicMock())
        with pytest.raises(ValueError):
            reader_builder.build()

    def test_build_with_valid_parameters(self):
        spark_session = MagicMock()
        reader_builder = (
            SparkReaderBuilder()
            .set_spark_session(spark_session)
            .set_format("json")
            .set_source_path("path/to/source")
            .set_read_mode("overwrite")
        )

        reader = reader_builder.build()
        assert reader.spark == spark_session
        assert reader.format == "json"
        assert reader.config["src_path"] == "path/to/source"
        assert reader.config["readMode"] == "overwrite"
