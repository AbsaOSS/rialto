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
from datetime import datetime, timedelta

import pytest

from rialto.runner.reporting.record import Record
from rialto.runner.services.config_loader import RunnerConfig
from rialto.runner.services.date_manager import DateManager


@pytest.fixture(scope="module")
def basic_date_manager() -> DateManager:
    cfg = RunnerConfig(watched_period_units="months", watched_period_value=4)
    return DateManager(cfg)


@pytest.fixture(scope="function")
def record(basic_date_manager):
    return Record(
        "job",
        "target",
        basic_date_manager.str_to_date("2024-01-01"),
        timedelta(days=0, hours=1, minutes=2, seconds=3),
        1,
        "status",
        "reason",
        None,
        datetime(2024, 1, 1, 1, 2, 3),
    )


def test_record_to_spark(spark, basic_date_manager, record):
    row = record.to_spark_row()
    assert row.job == "job"
    assert row.target == "target"
    assert row.date == basic_date_manager.str_to_date("2024-01-01")
    assert row.time == "1:02:03"
    assert row.records == 1
    assert row.status == "status"
    assert row.reason == "reason"
    assert row.exception is None
    assert row.run_timestamp == datetime(2024, 1, 1, 1, 2, 3)
