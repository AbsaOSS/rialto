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
from unittest.mock import Mock, patch

import pytest

from rialto.runner.table import Table
from rialto.runner.task_registry import PipelineDependency
from rialto.runner.task_status_checker import TaskStatusChecker


def make_task(op="my_pipeline", partition_date=date(2020, 1, 1), dependencies=None):
    task = Mock()
    task.op = op
    task.partition_date = partition_date
    task.target = Table(schema_path="cat.sch", class_name="TestClass", partition="part")
    task.dependencies = dependencies or []
    task.completion = False
    task.dependencies_complete = False
    return task


def make_dependency(table_path="cat.sch.dep_table", date_from=date(2019, 12, 1), date_until=date(2020, 1, 1)):
    table = Table(table_path=table_path, partition="part")
    return PipelineDependency(table=table, date_from=date_from, date_until=date_until)


@pytest.fixture
def mock_data_checker():
    return Mock()  # Mock of DataChecker


@pytest.fixture
def status_checker(mock_data_checker):
    return TaskStatusChecker(checker=mock_data_checker)


def test_check_completion_sets_true_when_data_exists(status_checker, mock_data_checker):
    mock_data_checker.check_date.return_value = True
    task = make_task()

    status_checker.check_completion(task)

    assert task.completion is True
    mock_data_checker.check_date.assert_called_once_with(task.target, task.partition_date)


def test_check_completion_sets_false_when_no_data(status_checker, mock_data_checker):
    mock_data_checker.check_date.return_value = False
    task = make_task()

    status_checker.check_completion(task)

    assert task.completion is False


# --- check_pipeline_dependencies ---


def test_check_pipeline_dependencies_no_deps(status_checker, mock_data_checker):
    task = make_task(dependencies=[])

    status_checker.check_pipeline_dependencies(task)

    assert task.dependencies_complete is True  # all([]) == True
    mock_data_checker.check_range.assert_not_called()


def test_check_pipeline_dependencies_all_complete(status_checker, mock_data_checker):
    mock_data_checker.check_range.return_value = True
    dep1 = make_dependency("cat.sch.table_a")
    dep2 = make_dependency("cat.sch.table_b")
    task = make_task(dependencies=[dep1, dep2])

    status_checker.check_pipeline_dependencies(task)

    assert dep1.complete is True
    assert dep2.complete is True
    assert task.dependencies_complete is True
    assert mock_data_checker.check_range.call_count == 2


def test_check_pipeline_dependencies_one_incomplete(status_checker, mock_data_checker):
    mock_data_checker.check_range.side_effect = [True, False]
    dep1 = make_dependency("cat.sch.table_a")
    dep2 = make_dependency("cat.sch.table_b")
    task = make_task(dependencies=[dep1, dep2])

    status_checker.check_pipeline_dependencies(task)

    assert dep1.complete is True
    assert dep2.complete is False
    assert task.dependencies_complete is False


def test_check_pipeline_dependencies_passes_correct_dates(status_checker, mock_data_checker):
    mock_data_checker.check_range.return_value = True
    dep = make_dependency(date_from=date(2019, 10, 1), date_until=date(2020, 1, 1))
    task = make_task(dependencies=[dep])

    status_checker.check_pipeline_dependencies(task)

    mock_data_checker.check_range.assert_called_once_with(dep.table, date(2019, 10, 1), date(2020, 1, 1))


def test_check_completion_logs_status():
    checker = Mock()
    checker.check_date.return_value = True
    task = make_task(op="logged_pipeline", partition_date=date(2020, 3, 1))
    status_checker = TaskStatusChecker(checker=checker)

    with patch("rialto.runner.task_status_checker.logger") as mock_logger:
        status_checker.check_completion(task)

    mock_logger.info.assert_called_once()
    log_msg = mock_logger.info.call_args[0][0]
    assert "logged_pipeline" in log_msg
    assert "2020-03-01" in log_msg
    assert "True" in log_msg
