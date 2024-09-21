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
from datetime import datetime

import pytest

from rialto.runner.config_loader import IntervalConfig, ScheduleConfig
from rialto.runner.date_manager import DateManager


def test_str_to_date():
    assert DateManager.str_to_date("2023-03-05") == datetime.strptime("2023-03-05", "%Y-%m-%d").date()


@pytest.mark.parametrize(
    "units , value, res",
    [("days", 7, "2023-02-26"), ("weeks", 3, "2023-02-12"), ("months", 5, "2022-10-05"), ("years", 2, "2021-03-5")],
)
def test_date_from(units, value, res):
    rundate = DateManager.str_to_date("2023-03-05")
    date_from = DateManager.date_subtract(run_date=rundate, units=units, value=value)
    assert date_from == DateManager.str_to_date(res)


def test_date_from_bad():
    rundate = DateManager.str_to_date("2023-03-05")
    with pytest.raises(ValueError) as exception:
        DateManager.date_subtract(run_date=rundate, units="random", value=1)
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


def test_run_dates_weekly():
    cfg = ScheduleConfig(frequency="weekly", day=5)

    run_dates = DateManager.run_dates(
        date_from=DateManager.str_to_date("2023-02-05"),
        date_to=DateManager.str_to_date("2023-04-07"),
        schedule=cfg,
    )

    expected = [
        "2023-02-10",
        "2023-02-17",
        "2023-02-24",
        "2023-03-03",
        "2023-03-10",
        "2023-03-17",
        "2023-03-24",
        "2023-03-31",
        "2023-04-07",
    ]
    expected = [DateManager.str_to_date(d) for d in expected]
    assert run_dates == expected


def test_run_dates_monthly():
    cfg = ScheduleConfig(frequency="monthly", day=5)

    run_dates = DateManager.run_dates(
        date_from=DateManager.str_to_date("2022-08-05"),
        date_to=DateManager.str_to_date("2023-04-07"),
        schedule=cfg,
    )

    expected = [
        "2022-08-05",
        "2022-09-05",
        "2022-10-05",
        "2022-11-05",
        "2022-12-05",
        "2023-01-05",
        "2023-02-05",
        "2023-03-05",
        "2023-04-05",
    ]
    expected = [DateManager.str_to_date(d) for d in expected]
    assert run_dates == expected


def test_run_dates_daily():
    cfg = ScheduleConfig(frequency="daily")

    run_dates = DateManager.run_dates(
        date_from=DateManager.str_to_date("2023-03-28"),
        date_to=DateManager.str_to_date("2023-04-03"),
        schedule=cfg,
    )

    expected = [
        "2023-03-28",
        "2023-03-29",
        "2023-03-30",
        "2023-03-31",
        "2023-04-01",
        "2023-04-02",
        "2023-04-03",
    ]
    expected = [DateManager.str_to_date(d) for d in expected]
    assert run_dates == expected


def test_run_dates_invalid():
    cfg = ScheduleConfig(frequency="random")
    with pytest.raises(ValueError) as exception:
        DateManager.run_dates(
            date_from=DateManager.str_to_date("2023-03-28"),
            date_to=DateManager.str_to_date("2023-04-03"),
            schedule=cfg,
        )
    assert str(exception.value) == "Unknown frequency random"


@pytest.mark.parametrize(
    "shift, res",
    [(7, "2023-02-26"), (3, "2023-03-02"), (-5, "2023-03-10"), (0, "2023-03-05")],
)
def test_to_info_date(shift, res):
    cfg = ScheduleConfig(frequency="daily", info_date_shift=[IntervalConfig(units="days", value=shift)])
    base = DateManager.str_to_date("2023-03-05")
    info = DateManager.to_info_date(base, cfg)
    assert DateManager.str_to_date(res) == info


@pytest.mark.parametrize(
    "unit, result",
    [("days", "2023-03-02"), ("weeks", "2023-02-12"), ("months", "2022-12-05"), ("years", "2020-03-05")],
)
def test_info_date_shift_units(unit, result):
    cfg = ScheduleConfig(frequency="daily", info_date_shift=[IntervalConfig(units=unit, value=3)])
    base = DateManager.str_to_date("2023-03-05")
    info = DateManager.to_info_date(base, cfg)
    assert DateManager.str_to_date(result) == info


def test_info_date_shift_combined():
    cfg = ScheduleConfig(
        frequency="daily",
        info_date_shift=[IntervalConfig(units="months", value=3), IntervalConfig(units="days", value=4)],
    )
    base = DateManager.str_to_date("2023-03-05")
    info = DateManager.to_info_date(base, cfg)
    assert DateManager.str_to_date("2022-12-01") == info
