import os
import tempfile

import pytest
import yaml

from ingestion_engine.config import ReadConfig, TaskConfig, WriteConfig


class TestConfig:
    @pytest.fixture
    def config_file(self):
        config = {
            "task_name": "test_task",
            "owner": "Owner",
            "read_config": {
                "format": "parquet",
                "source_path": "source_path",
            },
            "write_config": {
                "format": "parquet",
                "write_mode": "overwrite",
                "target_path": "target_path",
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = os.path.join(tmp_dir, "test_config.yaml")

            with open(config_file, "w") as f:
                yaml.dump(config, f)

            yield config_file

    def test_read_config_from_file_works(self, config_file):
        result = TaskConfig.read_config_from_yaml(config_file)

        expected = TaskConfig(
            task_name="test_task",
            owner="Owner",
            read_config=ReadConfig(format="parquet", source_path="source_path"),
            write_config=WriteConfig(
                format="parquet", write_mode="overwrite", target_path="target_path"
            ),
        )

        assert result == expected

    def test_read_config_from_file_fails_when_a_required_argument_is_missing(self):
        config = {
            "task_name": "test_task",
            "owner": "Owner",
            "read_config": {"format": "parquet", "source_path": "source_path"},
            "write_config": {
                "format": "parquet",
                "write_mode": "overwrite",
                # Missing target_path
            },
        }

        with pytest.raises(ValueError) as e:
            TaskConfig(**config)

        assert "1 validation error for TaskConfig\nwrite_config.target_path" in str(
            e.value
        )
