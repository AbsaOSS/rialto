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
from unittest.mock import patch

import pytest

from rialto.runner.services.config_loader import (
    DependencyConfig,
    IntervalConfig,
    ModuleConfig,
    PipelineConfig,
    RunnerConfig,
    ScheduleConfig,
    TargetConfig,
)
from rialto.runner.services.date_manager import DateManager
from rialto.runner.services.task_registry import (
    PipelineDependency,
    PipelineTask,
    TaskRegistry,
)


@pytest.fixture(scope="module")
def date_manager():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=3)
    return DateManager(runner_cfg, "2020-01-01")


@pytest.fixture
def pipeline_config_no_deps():
    return PipelineConfig(
        name="test_pipeline",
        module=ModuleConfig(python_module="some.module", python_class="TestClass"),
        schedule=ScheduleConfig(frequency="monthly", day=1),
        target=TargetConfig(
            target_schema="cat.sch",
            target_partition_column="part",
            secondary_partition_columns=["sec1", "sec2"],
            custom_name=None,
            rerun_filters={"col": "value"},
        ),
    )


@pytest.fixture
def pipeline_config_with_deps():
    return PipelineConfig(
        name="test_pipeline_with_deps",
        module=ModuleConfig(python_module="some.module", python_class="TestClass"),
        schedule=ScheduleConfig(frequency="monthly", day=1),
        target=TargetConfig(
            target_schema="cat.sch",
            target_partition_column="part",
        ),
        dependencies=[
            DependencyConfig(
                table="cat.sch.dep_table",
                date_col="part",
                interval=IntervalConfig(units="months", value=1),
            )
        ],
    )


def test_registry_initializes_empty(spark, date_manager):
    registry = TaskRegistry(spark, date_manager)
    assert list(registry) == []


def test_add_task_no_dependencies(spark, date_manager, pipeline_config_no_deps):
    registry = TaskRegistry(spark, date_manager)
    registry.add_task(
        name="test_pipeline",
        execution_date=date(2020, 1, 1),
        partition_date=date(2019, 12, 31),
        config=pipeline_config_no_deps,
    )

    tasks = list(registry)
    assert len(tasks) == 1

    task = tasks[0]
    assert isinstance(task, PipelineTask)
    assert task.op == "test_pipeline"
    assert task.execution_date == date(2020, 1, 1)
    assert task.partition_date == date(2019, 12, 31)
    assert task.config is pipeline_config_no_deps
    assert task.dependencies == []
    assert task.completion is False
    assert task.dependencies_complete is False


def test_add_task_target_table(spark, date_manager, pipeline_config_no_deps):
    registry = TaskRegistry(spark, date_manager)
    registry.add_task(
        name="test_pipeline",
        execution_date=date(2020, 1, 1),
        partition_date=date(2019, 12, 31),
        config=pipeline_config_no_deps,
    )

    task = list(registry)[0]
    assert task.target.catalog == "cat"
    assert task.target.schema == "sch"
    assert task.target.partition == "part"
    assert task.target.secondary_partitions == ["sec1", "sec2"]
    assert task.target.filters == {"col": "value"}


def test_add_task_with_dependencies(spark, date_manager, pipeline_config_with_deps):
    registry = TaskRegistry(spark, date_manager)
    registry.add_task(
        name="test_pipeline_with_deps",
        execution_date=date(2020, 1, 1),
        partition_date=date(2019, 12, 31),
        config=pipeline_config_with_deps,
    )

    task = list(registry)[0]
    assert len(task.dependencies) == 1

    dep = task.dependencies[0]
    assert isinstance(dep, PipelineDependency)
    assert dep.table.get_table_path() == "cat.sch.dep_table"
    assert dep.date_until == date(2020, 1, 1)
    assert dep.date_from == date(2019, 12, 1)  # 1 month subtracted
    assert dep.complete is False


def test_add_multiple_tasks(spark, date_manager, pipeline_config_no_deps, pipeline_config_with_deps):
    registry = TaskRegistry(spark, date_manager)
    registry.add_task("pipeline_a", date(2020, 1, 1), date(2019, 12, 31), pipeline_config_no_deps)
    registry.add_task("pipeline_b", date(2020, 1, 1), date(2019, 12, 31), pipeline_config_with_deps)

    tasks = list(registry)
    assert len(tasks) == 2
    assert tasks[0].op == "pipeline_a"
    assert tasks[1].op == "pipeline_b"


def test_iteration(spark, date_manager, pipeline_config_no_deps):
    registry = TaskRegistry(spark, date_manager)
    registry.add_task("p1", date(2020, 1, 1), date(2019, 12, 31), pipeline_config_no_deps)
    registry.add_task("p2", date(2020, 1, 1), date(2019, 12, 31), pipeline_config_no_deps)

    names = [task.op for task in registry]
    assert names == ["p1", "p2"]


def test_log_status_contains_task_info(spark, date_manager, pipeline_config_no_deps):
    registry = TaskRegistry(spark, date_manager)
    registry.add_task("test_pipeline", date(2020, 1, 1), date(2019, 12, 31), pipeline_config_no_deps)
    registry.tasks[0].completion = True

    with patch("rialto.runner.services.task_registry.logger") as mock_logger:
        registry.log_status()

    mock_logger.info.assert_called_once()
    logged_output = mock_logger.info.call_args[0][0]

    assert "test_pipeline" in logged_output
    assert "2019-12-31" in logged_output
    assert "✔" in logged_output
    assert "✘" in logged_output
