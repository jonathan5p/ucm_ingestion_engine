import os
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from ingestion_engine.config import ReadConfig, TaskConfig, WriteConfig
from ingestion_engine.engine import IngestionEngine
from ingestion_engine.readers.spark_reader import SparkReaderBuilder
from ingestion_engine.writers.spark_writer import SparkWriterBuilder


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.appName("test_app")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark
    spark.stop()


class TestIngestionEngine:
    def test_ingestion_engine_run_batch(self, spark):
        with tempfile.TemporaryDirectory() as tmp_dir:
            src_path = os.path.join(tmp_dir, "src_parquet")
            test_data = [
                {"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 35},
                {"name": "Eve   A.", "age": 28},
            ]

            test_df = spark.createDataFrame(test_data)
            test_df.write.format("parquet").mode("overwrite").save(src_path)

            schema_file = os.path.join(tmp_dir, "test_dataset.schema.json")
            with open(schema_file, "wb") as f:
                f.write(test_df.schema.json().encode())

            task_config = TaskConfig(
                task_name="engine_test",
                owner="Owner",
                read_config=ReadConfig(
                    source_path=src_path,
                    format="parquet",
                    schema_path=schema_file,
                ),
                write_config=WriteConfig(
                    target_path=os.path.join(tmp_dir, "target_parquet"),
                    format="parquet",
                    write_mode="append",
                ),
            )

            reader = (
                SparkReaderBuilder()
                .set_source_path(task_config.read_config.source_path)
                .set_format(task_config.read_config.format)
                .set_schema_path(task_config.read_config.schema_path)
                .set_spark_session(spark)
                .build()
            )

            writer = (
                SparkWriterBuilder()
                .set_target_path(task_config.write_config.target_path)
                .set_write_mode(task_config.write_config.write_mode)
                .set_format(task_config.write_config.format)
                .set_etl_mode(task_config.write_config.etl_mode)
                .set_spark_session(spark)
                .build()
            )

            engine = IngestionEngine(reader, writer, task_config)
            engine.run()
