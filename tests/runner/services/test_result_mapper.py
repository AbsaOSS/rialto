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
from datetime import date, datetime, timedelta
from unittest.mock import Mock

from rialto.runner.services.result_mapper import TaskResultMapper


def _make_task(op="my_pipeline", table_path="catalog.schema.table", partition_date=date(2026, 1, 1)):
    task = Mock()
    task.op = op
    task.partition_date = partition_date
    task.target.get_table_path.return_value = table_path
    return task


def _run_start():
    return datetime.now() - timedelta(seconds=5)


# ── success ──────────────────────────────────────────────────────────────────


def test_success():
    task = _make_task(op="pipe_a", table_path="cat.sch.tbl", partition_date=date(2026, 5, 1))
    record = TaskResultMapper.success(task, _run_start(), records_count=42)
    assert record.status == "Success"
    assert record.reason == "OK"
    assert record.exception is None
    assert record.records == 42
    assert record.job == "pipe_a"
    assert record.target == "cat.sch.tbl"
    assert record.date == date(2026, 5, 1)
    assert isinstance(record.time, timedelta)
    assert record.time.total_seconds() > 0


# ── already_complete ──────────────────────────────────────────────────────────


def test_already_complete():
    task = _make_task(op="pipe_b", table_path="cat.sch.tbl2", partition_date=date(2026, 3, 15))
    record = TaskResultMapper.already_complete(task, _run_start())
    assert record.status == "Skipped"
    assert record.reason == "AlreadyComplete"
    assert record.exception is None
    assert record.records == 0
    assert record.job == "pipe_b"
    assert record.target == "cat.sch.tbl2"
    assert record.date == date(2026, 3, 15)


# ── dependencies_incomplete ───────────────────────────────────────────────────


def test_dependencies_incomplete_status_and_reason():
    task = _make_task(op="pipe_b", table_path="cat.sch.tbl2", partition_date=date(2026, 3, 15))
    failed = ["cat.s.dep1 from 2026-01-01 until 2026-01-07", "cat.s.dep2 from 2026-01-01 until 2026-01-07"]
    record = TaskResultMapper.dependencies_incomplete(task, _run_start(), failed)
    assert record.status == "Failed"
    assert record.reason == "Dependencies Incomplete"
    assert record.records == 0
    assert record.job == "pipe_b"
    assert record.target == "cat.sch.tbl2"
    assert record.date == date(2026, 3, 15)
    for dep in failed:
        assert dep in record.exception


def test_dependencies_incomplete_lists_failed_deps_in_exception():
    failed = ["cat.s.dep1 from 2026-01-01 until 2026-01-07", "cat.s.dep2 from 2026-01-01 until 2026-01-07"]
    record = TaskResultMapper.dependencies_incomplete(_make_task(), _run_start(), failed)
    for dep in failed:
        assert dep in record.exception


def test_dependencies_incomplete_empty_list_falls_back():
    record = TaskResultMapper.dependencies_incomplete(_make_task(), _run_start(), [])
    assert record.exception == "Unknown"


# ── exception ─────────────────────────────────────────────────────────────────


def test_exception():
    task = _make_task(op="pipe_c", table_path="cat.sch.tbl3", partition_date=date(2026, 4, 10))
    record = TaskResultMapper.exception(task, _run_start(), "ValueError", "Traceback...")
    assert record.status == "Error"
    assert record.reason == "ValueError"
    assert record.exception == "Traceback..."
    assert record.records == 0
    assert record.job == "pipe_c"
    assert record.target == "cat.sch.tbl3"
    assert record.date == date(2026, 4, 10)


# ── interrupted ───────────────────────────────────────────────────────────────


def test_interrupted():
    task = _make_task(op="pipe_c", table_path="cat.sch.tbl3", partition_date=date(2026, 4, 10))
    record = TaskResultMapper.interrupted(task, _run_start())
    assert record.status == "Error"
    assert record.reason == "Keyboard Interrupt"
    assert record.exception is None
    assert record.records == 0
    assert record.job == "pipe_c"
    assert record.target == "cat.sch.tbl3"
    assert record.date == date(2026, 4, 10)
