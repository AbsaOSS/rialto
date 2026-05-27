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
# tests/runner/test_services.py
from datetime import date
from unittest.mock import Mock, patch

from rialto.runner.reporting.tracker import Tracker
from rialto.runner.runner_services import DefaultRunnerServices, RunnerServices
from rialto.runner.services.data_checker import DataChecker
from rialto.runner.services.date_manager import DateManager
from rialto.runner.services.executor import PipelineExecutor
from rialto.runner.services.task_registry import TaskRegistry
from rialto.runner.services.task_status_checker import TaskStatusChecker
from rialto.runner.services.writer import DatabricksWriter

CONFIG_PATH = "tests/runner/resources/config.yaml"


# ── RunnerServices dataclass ──────────────────────────────────────────────────


def test_runner_services_stores_all_fields():
    """RunnerServices holds all injected collaborators as attributes"""
    config = Mock()
    date_manager = Mock()
    writer = Mock()
    data_checker = Mock()
    task_checker = Mock()
    registry = Mock()
    executor = Mock()
    tracker = Mock()

    services = RunnerServices(
        config=config,
        date_manager=date_manager,
        writer=writer,
        data_checker=data_checker,
        task_checker=task_checker,
        registry=registry,
        executor=executor,
        tracker=tracker,
    )

    assert services.config is config
    assert services.date_manager is date_manager
    assert services.writer is writer
    assert services.data_checker is data_checker
    assert services.task_checker is task_checker
    assert services.registry is registry
    assert services.executor is executor
    assert services.tracker is tracker


# ── DefaultRunnerServices.build ───────────────────────────────────────────────


def test_build_returns_runner_services_instance(spark):
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH)
    assert isinstance(services, RunnerServices)


def test_build_creates_correct_types(spark):
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH)
    assert isinstance(services.date_manager, DateManager)
    assert isinstance(services.writer, DatabricksWriter)
    assert isinstance(services.data_checker, DataChecker)
    assert isinstance(services.task_checker, TaskStatusChecker)
    assert isinstance(services.registry, TaskRegistry)
    assert isinstance(services.executor, PipelineExecutor)
    assert isinstance(services.tracker, Tracker)


def test_build_passes_merge_schema_to_writer(spark):
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH, merge_schema=True)
    assert services.writer.merge_schema is True


def test_build_merge_schema_default_is_false(spark):
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH)
    assert services.writer.merge_schema is False


def test_build_shares_data_checker_between_task_checker_and_executor(spark):
    """data_checker should be the same instance in task_checker and executor"""
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH)
    assert services.executor.checker is services.data_checker


def test_build_config_loads_pipelines(spark):
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH)
    pipeline_names = [p.name for p in services.config.pipelines]
    assert "SimpleGroup" in pipeline_names


def test_build_with_run_date_passed_to_date_manager(spark):
    """run_date is passed to DateManager — verify it doesn't raise and config is loaded"""
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH, run_date="2023-03-31")
    assert isinstance(services.date_manager, DateManager)
    assert services.date_manager.date_until == date(2023, 3, 31)


def test_build_tracker_has_bookkeeper_when_config_has_bookkeeping(spark):
    """config.yaml sets bookkeeping: some.test.location — tracker should have a bookkeeper"""
    services = DefaultRunnerServices.build(spark=spark, config_path=CONFIG_PATH)
    assert services.tracker.bookkeeper is not None


def test_build_calls_config_loader_with_overrides(spark):
    overrides = {"runner": {"watched_period_units": "days", "watched_period_value": 7}}
    with patch("rialto.runner.runner_services.ConfigLoader") as config_loader_cls, patch(
        "rialto.runner.runner_services.DateManager", Mock()
    ):
        DefaultRunnerServices.build(
            spark=spark,
            config_path=CONFIG_PATH,
            overrides=overrides,
        )
        config_loader_cls.load_yaml.assert_called_once_with(CONFIG_PATH, overrides)
