import logging

from ingestion_engine.config import TaskConfig
from ingestion_engine.readers.base_reader import BaseReader
from ingestion_engine.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IngestionEngine:
    def __init__(
        self, reader: BaseReader, writer: BaseWriter, task_config: TaskConfig
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.config = task_config

    def run(self):
        logger.info(f"Starting ingestion task: {self.config.task_name}")
        df = self.reader.read()
        logger.info(
            f"Data read successfully from {self.config.read_config.source_path}"
        )
        df = df.transform(self.writer.add_metadata)
        logger.info(
            f"Metadata added successfully to the DataFrame. Writing to {self.config.write_config.target_path}"
        )
        self.writer.write(df)
