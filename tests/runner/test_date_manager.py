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
from datetime import date, datetime

import pytest

from rialto.runner.config_loader import IntervalConfig, RunnerConfig, ScheduleConfig
from rialto.runner.date_manager import DateManager


def test_str_to_date():
    assert DateManager.str_to_date("2023-03-05") == datetime.strptime("2023-03-05", "%Y-%m-%d").date()


@pytest.mark.parametrize(
    "units , value, res",
    [("days", 7, "2023-02-26"), ("weeks", 3, "2023-02-12"), ("months", 5, "2022-10-05"), ("years", 2, "2021-03-5")],
)
def test_date_subtract(units, value, res):
    rundate = DateManager.str_to_date("2023-03-05")
    date_from = DateManager.date_subtract(input_date=rundate, units=units, value=value)
    assert date_from == DateManager.str_to_date(res)


def test_date_subtract_bad():
    rundate = DateManager.str_to_date("2023-03-05")
    with pytest.raises(ValueError) as exception:
        DateManager.date_subtract(input_date=rundate, units="random", value=1)
    assert str(exception.value) == "Unknown time unit random"


def test_all_dates():
    all_dates = DateManager.all_dates(
        date_from=DateManager.str_to_date("2023-02-05"),
        date_to=DateManager.str_to_date("2023-04-12"),
    )
    assert len(all_dates) == 67
    assert all_dates[1] == DateManager.str_to_date("2023-02-06")


def test_all_dates_reversed():
    all_dates = DateManager.all_dates(
        date_from=DateManager.str_to_date("2023-04-12"),
        date_to=DateManager.str_to_date("2023-02-05"),
    )
    assert len(all_dates) == 67
    assert all_dates[1] == DateManager.str_to_date("2023-02-06")


def test_date_from():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=3)
    date_manager = DateManager(config=runner_cfg, run_date="2023-03-05")
    assert date_manager.get_date_from() == DateManager.str_to_date("2022-12-05")


def test_date_to():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=3)
    date_manager = DateManager(config=runner_cfg, run_date="2023-03-05")
    assert date_manager.get_date_until() == DateManager.str_to_date("2023-03-05")


def test_run_dates_daily_no_shift():
    runner_cfg = RunnerConfig(watched_period_units="weeks", watched_period_value=1)
    cfg = ScheduleConfig(frequency="daily")
    manager = DateManager(config=runner_cfg, run_date="2026-05-20")

    exec, part = zip(*manager.get_execution_and_partition_dates(schedule=cfg))

    expected_execution_dates = [
        date(2026, 5, 13),
        date(2026, 5, 14),
        date(2026, 5, 15),
        date(2026, 5, 16),
        date(2026, 5, 17),
        date(2026, 5, 18),
        date(2026, 5, 19),
        date(2026, 5, 20),
    ]

    expected_partition_dates = [
        date(2026, 5, 13),
        date(2026, 5, 14),
        date(2026, 5, 15),
        date(2026, 5, 16),
        date(2026, 5, 17),
        date(2026, 5, 18),
        date(2026, 5, 19),
        date(2026, 5, 20),
    ]
    assert expected_execution_dates == list(exec)
    assert expected_partition_dates == list(part)


def test_run_dates_weekly_backwards_shift():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=1)
    cfg = ScheduleConfig(frequency="weekly", day=5, info_date_shift=IntervalConfig(units="days", value=2))
    manager = DateManager(config=runner_cfg, run_date="2026-05-20")

    exec, part = zip(*manager.get_execution_and_partition_dates(schedule=cfg))

    expected_execution_dates = [
        date(2026, 4, 24),
        date(2026, 5, 1),
        date(2026, 5, 8),
        date(2026, 5, 15),
    ]

    expected_partition_dates = [
        date(2026, 4, 22),
        date(2026, 4, 29),
        date(2026, 5, 6),
        date(2026, 5, 13),
    ]
    assert expected_execution_dates == list(exec)
    assert expected_partition_dates == list(part)


def test_run_dates_monthly_with_forward_shift():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=3)
    cfg = ScheduleConfig(frequency="monthly", day=5, info_date_shift=IntervalConfig(units="days", value=-2))
    manager = DateManager(config=runner_cfg, run_date="2026-05-20")

    exec, part = zip(*manager.get_execution_and_partition_dates(schedule=cfg))

    expected_execution_dates = [
        date(2026, 3, 5),
        date(2026, 4, 5),
        date(2026, 5, 5),
    ]

    expected_partition_dates = [
        date(2026, 3, 7),
        date(2026, 4, 7),
        date(2026, 5, 7),
    ]
    assert expected_execution_dates == list(exec)
    assert expected_partition_dates == list(part)


def test_run_dates_monthly_last():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=3)
    cfg = ScheduleConfig(frequency="monthly", day="last")
    manager = DateManager(config=runner_cfg, run_date="2026-05-20")

    exec, part = zip(*manager.get_execution_and_partition_dates(schedule=cfg))

    expected_execution_dates = [
        date(2026, 2, 28),
        date(2026, 3, 31),
        date(2026, 4, 30),
    ]

    expected_partition_dates = [
        date(2026, 2, 28),
        date(2026, 3, 31),
        date(2026, 4, 30),
    ]
    assert expected_execution_dates == list(exec)
    assert expected_partition_dates == list(part)


def test_invalid_days():
    runner_cfg = RunnerConfig(watched_period_units="months", watched_period_value=3)
    weekly_cfg = ScheduleConfig(frequency="weekly", day=12)
    monthly_cfg = ScheduleConfig(frequency="monthly", day=42)
    manager = DateManager(config=runner_cfg, run_date="2026-05-20")

    with pytest.raises(ValueError):
        manager.get_execution_and_partition_dates(schedule=weekly_cfg)

    with pytest.raises(ValueError):
        manager.get_execution_and_partition_dates(schedule=monthly_cfg)
