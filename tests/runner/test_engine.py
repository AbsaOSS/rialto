#  Copyright 2022-2026 ABSA Group Limited
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
from unittest.mock import MagicMock, call, patch

import pytest

from rialto.runner.engine import RunnerEngine
from rialto.runner.services.config_loader import (
    ModuleConfig,
    PipelineConfig,
    ScheduleConfig,
)


def _pipeline(name="p1", schedule="weekly"):
    sch_cfg = ScheduleConfig(frequency=schedule, day=1)
    return PipelineConfig(name=name, schedule=sch_cfg, module=ModuleConfig(python_module="mod", python_class="Class"))


def _dependency(complete=True, table_path="src.sch.dep", date_from=date(2026, 1, 1), date_until=date(2026, 1, 7)):
    dep = MagicMock()
    dep.complete = complete
    dep.date_from = date_from
    dep.date_until = date_until
    dep.table.get_table_path.return_value = table_path
    return dep


def _task(
    name="p1",
    completion=False,
    dependencies_complete=True,
    precheck_failed=False,
    partition_date=date(2026, 1, 8),
    execution_date=date(2026, 1, 8),
    deps=None,
):
    t = MagicMock()
    t.name = name
    t.completion = completion
    t.dependencies_complete = dependencies_complete
    t.precheck_failed = precheck_failed
    t.partition_date = partition_date
    t.execution_date = execution_date
    t.dependencies = deps if deps is not None else []
    t.target = MagicMock()
    return t


def _services():
    s = MagicMock()
    s.config = MagicMock()
    s.config.pipelines = [_pipeline("p1"), _pipeline("p2")]
    s.date_manager = MagicMock()
    s.registry = MagicMock()
    s.registry.tasks = []
    s.task_checker = MagicMock()
    s.executor = MagicMock()
    s.writer = MagicMock()
    s.data_checker = MagicMock()
    s.tracker = MagicMock()
    return s


# ---- select_pipelines ------------------------------------------------------


def test_select_pipelines_returns_all_when_op_none():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    result = engine.select_pipelines(None)

    assert result == services.config.pipelines


def test_select_pipelines_returns_matching_op():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    result = engine.select_pipelines("p2")

    assert len(result) == 1
    assert result[0].name == "p2"


def test_select_pipelines_raises_for_unknown_op():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with pytest.raises(ValueError, match="Unknown operation selected: nope"):
        engine.select_pipelines("nope")


# ---- register_tasks --------------------------------------------------------


def test_register_tasks_adds_task_for_each_execution_partition_pair():
    services = _services()
    services.date_manager.get_execution_and_partition_dates.return_value = [
        (date(2026, 1, 10), date(2026, 1, 8)),
        (date(2026, 1, 17), date(2026, 1, 15)),
    ]
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)
    pipelines = [_pipeline("p1", "weekly")]

    engine.register_tasks(pipelines)

    assert services.registry.add_task.call_count == 2
    services.registry.add_task.assert_has_calls(
        [
            call(
                name="p1",
                execution_date=date(2026, 1, 10),
                partition_date=date(2026, 1, 8),
                config=pipelines[0],
            ),
            call(
                name="p1",
                execution_date=date(2026, 1, 17),
                partition_date=date(2026, 1, 15),
                config=pipelines[0],
            ),
        ]
    )


# ---- check_tasks -----------------------------------------------------------


def test_check_tasks_calls_both_checks_by_default():
    services = _services()
    t1 = _task(name="p1")
    t2 = _task(name="p2")
    services.registry.tasks = [t1, t2]

    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)
    engine.check_tasks()

    services.task_checker.check_completion.assert_has_calls([call(t1), call(t2)])
    services.task_checker.check_pipeline_dependencies.assert_has_calls([call(t1), call(t2)])


def test_check_tasks_skips_completion_when_rerun_true():
    services = _services()
    t = _task()
    services.registry.tasks = [t]

    engine = RunnerEngine(services=services, rerun=True, skip_dependencies=False)
    engine.check_tasks()

    services.task_checker.check_completion.assert_not_called()
    services.task_checker.check_pipeline_dependencies.assert_called_once_with(t)


def test_check_tasks_skips_dependencies_when_skip_dependencies_true():
    services = _services()
    t = _task()
    services.registry.tasks = [t]

    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=True)
    engine.check_tasks()

    services.task_checker.check_completion.assert_called_once_with(t)
    services.task_checker.check_pipeline_dependencies.assert_not_called()


def test_check_completion_records_exception_and_sets_precheck_failed():
    services = _services()
    t = _task()
    services.registry.tasks = [t]
    services.task_checker.check_completion.side_effect = RuntimeError("boom")

    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)
    engine.check_tasks()

    services.task_checker.check_completion.assert_called_once_with(t)
    assert t.precheck_failed is True
    assert t.error == "boom"
    assert "RuntimeError: boom" in t.error_trace


def test_check_dependencies_records_exception_and_sets_precheck_failed():
    services = _services()
    t = _task()
    services.registry.tasks = [t]
    services.task_checker.check_pipeline_dependencies.side_effect = RuntimeError("boom")
    engine = RunnerEngine(services=services, rerun=True, skip_dependencies=False)
    engine.check_tasks()

    services.task_checker.check_pipeline_dependencies.assert_called_once_with(t)
    assert t.precheck_failed is True
    assert t.error == "boom"
    assert "RuntimeError: boom" in t.error_trace


# ---- run_tasks -------------------------------------------------------------


def test_run_tasks_calls_execute_with_tracking_for_each_task():
    services = _services()
    t1 = _task(name="p1")
    t2 = _task(name="p2")
    services.registry.tasks = [t1, t2]

    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch.object(engine, "_execute_task_with_tracking") as exec_track, patch(
        "rialto.runner.engine.logger.info"
    ) as log_info:
        engine.run_tasks()

    exec_track.assert_has_calls([call(t1), call(t2)])
    assert exec_track.call_count == 2
    assert log_info.call_count == 2


# ---- _execute_task_with_tracking branches ----------------------------------


def test_execute_task_with_tracking_records_precheck_failure_and_skips_execution():
    services = _services()
    task = _task(precheck_failed=True)
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch("rialto.runner.engine.TaskResultMapper.exception", return_value="rec") as mapper:
        engine._execute_task_with_tracking(task)

    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")
    services.executor.execute.assert_not_called()


def test_execute_task_with_tracking_skips_already_complete():
    services = _services()
    task = _task(completion=True, dependencies_complete=True, precheck_failed=False)
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch("rialto.runner.engine.TaskResultMapper.already_complete", return_value="rec") as mapper, patch(
        "rialto.runner.engine.logger.info"
    ):
        engine._execute_task_with_tracking(task)

    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")
    services.executor.execute.assert_not_called()


def test_execute_task_with_tracking_skips_incomplete_dependencies():
    services = _services()
    deps = [_dependency(complete=False, table_path="src.sch.dep1")]
    task = _task(completion=False, dependencies_complete=False, deps=deps)
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch("rialto.runner.engine.TaskResultMapper.dependencies_incomplete", return_value="rec") as mapper, patch(
        "rialto.runner.engine.logger.info"
    ):
        engine._execute_task_with_tracking(task)

    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")
    services.executor.execute.assert_not_called()


def test_execute_task_with_tracking_success_path():
    services = _services()
    task = _task(completion=False, dependencies_complete=True)
    services.executor.execute.return_value = "df"
    services.data_checker.check_written.return_value = 123
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch("rialto.runner.engine.TaskResultMapper.success", return_value="rec") as mapper, patch(
        "rialto.runner.engine.logger.info"
    ):
        engine._execute_task_with_tracking(task)

    services.executor.execute.assert_called_once_with(task)
    services.writer.write.assert_called_once_with("df", task.partition_date, task.target)
    services.data_checker.check_written.assert_called_once_with(task.target, task.partition_date, "df")
    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")


def test_execute_task_with_tracking_exception_path():
    services = _services()
    task = _task(completion=False, dependencies_complete=True)
    services.executor.execute.side_effect = RuntimeError("boom")
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with (
        patch("rialto.runner.engine.TaskResultMapper.exception", return_value="rec") as mapper,
        patch("rialto.runner.engine.logger.exception") as log_exc,
    ):
        engine._execute_task_with_tracking(task)

    log_exc.assert_called_once()
    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")
    services.writer.write.assert_not_called()


def test_execute_task_with_tracking_keyboard_interrupt_records_and_reraises():
    services = _services()
    task = _task(completion=False, dependencies_complete=True)
    services.executor.execute.side_effect = KeyboardInterrupt()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch("rialto.runner.engine.TaskResultMapper.interrupted", return_value="rec") as mapper:
        with pytest.raises(KeyboardInterrupt):
            engine._execute_task_with_tracking(task)

    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")


def test_execute_task_with_tracking_runs_when_skip_dependencies_true_even_if_incomplete():
    services = _services()
    deps = [_dependency(complete=False)]
    task = _task(completion=False, dependencies_complete=False, deps=deps)
    services.executor.execute.return_value = "df"
    services.data_checker.check_written.return_value = 1
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=True)

    with patch("rialto.runner.engine.TaskResultMapper.success", return_value="rec") as success_mapper, patch(
        "rialto.runner.engine.TaskResultMapper.dependencies_incomplete"
    ) as dep_mapper, patch("rialto.runner.engine.logger.info"):
        engine._execute_task_with_tracking(task)

    dep_mapper.assert_not_called()
    success_mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")


def test_execute_task_with_tracking_runs_when_rerun_true_even_if_completion_true():
    services = _services()
    task = _task(completion=True, dependencies_complete=True)
    services.executor.execute.return_value = "df"
    services.data_checker.check_written.return_value = 10
    engine = RunnerEngine(services=services, rerun=True, skip_dependencies=False)

    with patch("rialto.runner.engine.TaskResultMapper.success", return_value="rec") as mapper, patch(
        "rialto.runner.engine.logger.info"
    ):
        engine._execute_task_with_tracking(task)

    mapper.assert_called_once()
    services.tracker.add.assert_called_once_with("rec")


# ---- wrappers --------------------------------------------------------------


def test_finalize_calls_tracker_report_by_mail_and_log():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch.object(engine, "log_task_status") as log_task_status:
        engine.finalize()

    services.tracker.report_by_mail.assert_called_once_with()
    log_task_status.assert_called_once_with()


def test_run_calls_full_flow():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch.object(engine, "select_pipelines", return_value=["pipes"]) as select_pipes, patch.object(
        engine, "register_tasks"
    ) as register_tasks, patch.object(engine, "check_tasks") as check_tasks, patch.object(
        engine, "log_task_status"
    ) as log_status, patch.object(
        engine, "run_tasks"
    ) as run_tasks, patch.object(
        engine, "finalize"
    ) as finalize:
        engine.run("my_op")

    select_pipes.assert_called_once_with("my_op")
    register_tasks.assert_called_once_with(["pipes"])
    check_tasks.assert_called_once_with()
    log_status.assert_called_once_with()
    run_tasks.assert_called_once_with()
    finalize.assert_called_once_with()


def test_dry_run_execution_calls_expected_flow_without_run_or_finalize():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with patch.object(engine, "select_pipelines", return_value=["pipes"]) as select_pipes, patch.object(
        engine, "register_tasks"
    ) as register_tasks, patch.object(engine, "check_tasks") as check_tasks, patch.object(
        engine, "log_task_status"
    ) as log_status, patch.object(
        engine, "run_tasks"
    ) as run_tasks, patch.object(
        engine, "finalize"
    ) as finalize:
        engine.dry_run_execution("my_op")

    select_pipes.assert_called_once_with("my_op")
    register_tasks.assert_called_once_with(["pipes"])
    check_tasks.assert_called_once_with()
    log_status.assert_called_once_with()
    run_tasks.assert_not_called()
    finalize.assert_not_called()


def test_debug_first_task_registers_and_executes_first_task():
    services = _services()
    t1 = _task(name="first")
    t2 = _task(name="second")
    services.registry.tasks = [t1, t2]
    services.executor.execute.return_value = "df_debug"

    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    with (
        patch.object(engine, "select_pipelines", return_value=["pipes"]) as select_pipes,
        patch.object(engine, "register_tasks") as register_tasks,
    ):
        result = engine.debug_first_task("my_op")

    select_pipes.assert_called_once_with("my_op")
    register_tasks.assert_called_once_with(["pipes"])
    services.executor.execute.assert_called_once_with(t1)
    assert result == "df_debug"


# ---- logging ---------------------------------------------------------------
def test_log_task_status_calls_registry_log_status():
    services = _services()
    engine = RunnerEngine(services=services, rerun=False, skip_dependencies=False)

    engine.log_task_status()

    services.registry.log_status.assert_called_once_with()
