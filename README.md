Certainly! Here's a formatted and improved version of your README:

# Ingestion Engine

## Table of Contents
- [1. Structure of the Project](#1-structure-of-the-project)
- [2. How to Use It in a Simple Batch ETL](#2-how-to-use-it-in-a-simple-batch-etl)
- [3. How to Set Up the Project for Development](#3-how-to-set-up-the-project-for-development)

## 1. Structure of the Project

The project is organized as follows:

```
.
├── examples/
│   └── kafka/
│       └── kafka_etl.py
├── src/
│   └── ingestion_engine/
│       ├── __init__.py
│       ├── config.py
│       ├── engine.py
│       ├── readers/
│       │   ├── __init__.py
│       │   ├── base_reader.py
│       │   └── spark_reader.py
│       ├── writers/
│       │   ├── __init__.py
│       │   ├── base_writer.py
│       │   └── spark_writer.py
├── tests/
│   ├── test_config.py
│   ├── test_ingestion_engine.py
│   ├── readers/
│   │   └── test_spark_reader.py
│   └── writers/
│       └── test_spark_writer.py
├── Makefile
├── pyproject.toml
├── README.md
├── uv.lock
```

## 2. Examples

### Simple Batch ETL

This project provides a generic ingestion engine for batch ETL tasks. Below is an example of how to use it:

```python
from ingestion_engine.config import TaskConfig, ReadConfig, WriteConfig
from ingestion_engine.engine import IngestionEngine
from ingestion_engine.readers.spark_reader import SparkReaderBuilder
from ingestion_engine.writers.spark_writer import SparkWriterBuilder
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("BatchETL").getOrCreate()

# Define task configuration
task_config = TaskConfig(
    task_name="example_batch_etl",
    owner="Your Name",
    read_config=ReadConfig(
        source_path="path/to/source",
        format="parquet",
        schema_path="path/to/schema.json",
    ),
    write_config=WriteConfig(
        target_path="path/to/target",
        format="parquet",
        write_mode="append",
    ),
)

# Build reader and writer
reader = (
    SparkReaderBuilder()
    .set_spark_session(spark)
    .set_source_path(task_config.read_config.source_path)
    .set_format(task_config.read_config.format)
    .set_schema_path(task_config.read_config.schema_path)
    .build()
)

writer = (
    SparkWriterBuilder()
    .set_spark_session(spark)
    .set_target_path(task_config.write_config.target_path)
    .set_format(task_config.write_config.format)
    .set_write_mode(task_config.write_config.write_mode)
    .build()
)

# Run the ingestion engine
engine = IngestionEngine(reader, writer, task_config)
engine.run()
```

### Batch ETL Reading Config from Config File

This example demonstrates how to read configuration from a YAML file and use it to perform a batch ETL task. The configuration file will contain the necessary details for both the reader and the writer.

#### Example YAML Configuration File (`config.yaml`)

```yaml
task_name: "example_batch_etl"
owner: "Your Name"
read_config:
  format: "parquet"
  source_path: "path/to/source"
  schema_path: "path/to/schema.json"
write_config:
  target_path: "path/to/target"
  format: "parquet"
  write_mode: "append"
```

#### Batch ETL Script

```python
from ingestion_engine.config import TaskConfig, ReadConfig, WriteConfig
from ingestion_engine.engine import IngestionEngine
from ingestion_engine.readers.spark_reader import SparkReaderBuilder
from ingestion_engine.writers.spark_writer import SparkWriterBuilder
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("BatchETL").getOrCreate()

# Read task configuration from YAML file
config_file = "config_path/config.yaml"
task_config = TaskConfig.read_config_from_yaml(config_file)

# Build reader and writer
reader = (
    SparkReaderBuilder()
    .set_spark_session(spark)
    .set_format(task_config.read_config.format)
    .set_source_path(task_config.read_config.source_path)
    .set_schema_path(task_config.read_config.schema_path)
    .build()
)

writer = (
    SparkWriterBuilder()
    .set_spark_session(spark)
    .set_target_path(task_config.write_config.target_path)
    .set_format(task_config.write_config.format)
    .set_write_mode(task_config.write_config.write_mode)
    .build()
)

# Run the ingestion engine
engine = IngestionEngine(reader, writer, task_config)
engine.run()
```

The ```read_config_from_yaml``` method in the BaseConfig class is designed to read a YAML file and populate the TaskConfig object with the necessary details for the ETL process. This method assumes that the YAML file is structured correctly and contains all the required fields with the same names as they are defined in the corresponding config class definitions (TaskConfig, ReadConfig, WriteConfig).

### Kafka ETL Example

This project supports Kafka as a data source for ETL tasks. Below is an example of how to read data from a Kafka topic, process it, and write the results to a target location.


```python
from ingestion_engine.config import TaskConfig, ReadConfig, WriteConfig
from ingestion_engine.engine import IngestionEngine
from ingestion_engine.readers.spark_reader import SparkReaderBuilder
from ingestion_engine.writers.spark_writer import SparkWriterBuilder
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaETL").getOrCreate()

# Define task configuration
task_config = TaskConfig(
    task_name="kafka_etl_example",
    owner="Your Name",
    read_config=ReadConfig(
        format="kafka",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="sensor-telemetry",
        kafka_starting_offsets="earliest",
        kafka_value_format="avro",
        kafka_value_schema="""
        {
            "namespace": "com.farmia.iot",
            "name": "SensorTelemetry",
            "type": "record",
            "fields": [
                {
                    "name": "sensor_id",
                    "doc": "Unique identifier for the sensor",
                    "type": "string"
                },
                {
                    "name": "temperature",
                    "doc": "Temperature in degrees Celsius",
                    "type": "float"
                },
                {
                    "name": "humidity",
                    "doc": "Relative humidity in percentage",
                    "type": "float"
                },
                {
                    "name": "soil_fertility",
                    "doc": "Soil fertility in percentage",
                    "type": "float"
                },
                {
                    "name": "timestamp",
                    "doc": "Timestamp of the sensor data",
                    "type": "long",
                    "logicalType": "timestamp-millis"
                }
            ]
        }
        """
    ),
    write_config=WriteConfig(
        target_path="path/to/target",
        format="parquet",
        write_mode="append"
    ),
)

# Build reader and writer
reader = (
    SparkReaderBuilder()
    .set_spark_session(spark)
    .set_format(task_config.read_config.format)
    .set_kafka_bootstrap_servers(task_config.read_config.kafka_bootstrap_servers)
    .set_kafka_topic(task_config.read_config.kafka_topic)
    .set_kafka_starting_offsets(task_config.read_config.kafka_starting_offsets)
    .set_kafka_value_format(task_config.read_config.kafka_value_format)
    .set_kafka_value_schema(task_config.read_config.kafka_value_schema)
    .build()
)

writer = (
    SparkWriterBuilder()
    .set_spark_session(spark)
    .set_target_path(task_config.write_config.target_path)
    .set_format(task_config.write_config.format)
    .set_write_mode(task_config.write_config.write_mode)
    .build()
)

# Run the ingestion engine
engine = IngestionEngine(reader, writer, task_config)
engine.run()
```

## 3. How to Set Up the Project for Development

### Prerequisites
- Python 3.11 or higher

### Steps to Set Up the Project

1. **Clone the Repository:**

   ```sh
   git clone https://github.com/jonathan5p/ingestion-engine.git
   cd ingestion-engine
   ```

2. **Install uv**
    Follow the instructions in the [uv Installation Guide](https://docs.astral.sh/uv/getting-started/installation/)

3. **Install Dependencies:**

   ```sh
   uv sync
   ```

   **Note**: Before running this command you to have your virtual environment activated.

4. **Install Development Dependencies:**

   ```sh
   uv sync --dev
   ```


5. ** Run your python script:**

   ```sh
   uv run examples/kafka/kafka_etl.py
   ```

6. **Run Tests:**

   Use the Makefile to run tests:

   ```sh
   make test
   ```

7. **Format Code:**

   Use the Makefile to lint and format the code:

   ```sh
   make format
   ```

8. **Build the project:**

   ```sh
   uv build
   ```