#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from rialto.runner.services.executor import PipelineExecutor
from rialto.runner.services.task_registry import PipelineTask


def _make_executor():
    return PipelineExecutor(
        spark=MagicMock(),
        reader=MagicMock(),
        checker=MagicMock(),
    )


def _make_task():
    config = MagicMock()
    config.module = MagicMock()
    config.module.python_module = "tests.runner.transformations"
    config.module.python_class = "SimpleGroup"
    config.metadata_manager = None
    config.feature_loader = None

    task = PipelineTask(
        name="SimpleGroup",
        execution_date=date(2026, 1, 1),
        partition_date=date(2026, 1, 1),
        config=config,
        target=MagicMock(),
    )
    return task


def test_load_module_imports_and_instantiates_class():
    executor = _make_executor()
    cfg = MagicMock()
    cfg.python_module = "fake.module"
    cfg.python_class = "FakeClass"

    fake_module = MagicMock()
    fake_class = MagicMock()
    fake_instance = MagicMock()
    fake_class.return_value = fake_instance
    setattr(fake_module, "FakeClass", fake_class)

    with patch("rialto.runner.services.executor.import_module", return_value=fake_module) as import_module_mock:
        result = executor._load_module(cfg)

    import_module_mock.assert_called_once_with("fake.module")
    fake_class.assert_called_once_with()
    assert result is fake_instance


def test_init_tools_returns_none_when_not_configured():
    executor = _make_executor()

    pipeline_cfg = MagicMock()
    pipeline_cfg.metadata_manager = None
    pipeline_cfg.feature_loader = None

    metadata_manager, feature_loader = executor._init_tools(executor.spark, pipeline_cfg)

    assert metadata_manager is None
    assert feature_loader is None


def test_init_tools_creates_both_tools_when_configured():
    executor = _make_executor()

    pipeline_cfg = MagicMock()
    pipeline_cfg.metadata_manager = MagicMock()
    pipeline_cfg.metadata_manager.metadata_schema = "meta_schema"
    pipeline_cfg.feature_loader = MagicMock()
    pipeline_cfg.feature_loader.feature_schema = "feature_schema"
    pipeline_cfg.feature_loader.metadata_schema = "feature_meta_schema"

    with patch("rialto.runner.services.executor.MetadataManager") as metadata_manager_cls, patch(
        "rialto.runner.services.executor.PysparkFeatureLoader"
    ) as feature_loader_cls:
        metadata_manager, feature_loader = executor._init_tools(executor.spark, pipeline_cfg)

    metadata_manager_cls.assert_called_once_with(executor.spark, "meta_schema")
    feature_loader_cls.assert_called_once_with(
        executor.spark,
        feature_schema="feature_schema",
        metadata_schema="feature_meta_schema",
    )
    assert metadata_manager is metadata_manager_cls.return_value
    assert feature_loader is feature_loader_cls.return_value


def test_execute_calls_job_run_and_returns_df():
    executor = _make_executor()
    task = _make_task()

    mock_job = MagicMock()
    mock_df = MagicMock()
    mock_job.run.return_value = mock_df

    with patch.object(executor, "_load_module", return_value=mock_job) as load_module_mock, patch.object(
        executor, "_init_tools", return_value=(None, None)
    ) as init_tools_mock:
        result = executor.execute(task)

    load_module_mock.assert_called_once_with(task.config.module)
    init_tools_mock.assert_called_once_with(executor.spark, task.config)
    mock_job.run.assert_called_once_with(
        spark=executor.spark,
        run_date=task.execution_date,
        config=task.config,
        reader=executor.reader,
        metadata_manager=None,
        feature_loader=None,
    )
    assert result is mock_df


def test_execute_logs_start_of_execution():
    executor = _make_executor()
    task = _make_task()

    mock_job = MagicMock()
    mock_job.run.return_value = MagicMock()

    with patch("rialto.runner.services.executor.logger.info") as logger_info_mock, patch.object(
        executor, "_load_module", return_value=mock_job
    ), patch.object(executor, "_init_tools", return_value=(None, None)):
        executor.execute(task)

    logger_info_mock.assert_called_once_with(f"Executing pipeline {task.name} for partition date {task.partition_date}")


def test_execute_raises_when_job_run_fails():
    executor = _make_executor()
    task = _make_task()

    mock_job = MagicMock()
    mock_job.run.side_effect = RuntimeError("job failed")

    with patch.object(executor, "_load_module", return_value=mock_job), patch.object(
        executor, "_init_tools", return_value=(None, None)
    ):
        with pytest.raises(RuntimeError, match="job failed"):
            executor.execute(task)
