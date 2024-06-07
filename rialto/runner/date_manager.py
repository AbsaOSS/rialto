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

__all__ = ["DateManager"]

from datetime import date, datetime
from typing import List

from dateutil.relativedelta import relativedelta

from rialto.runner.config_loader import ScheduleConfig


class DateManager:
    """Date generation and shifts based on configuration"""

    @staticmethod
    def str_to_date(str_date: str) -> date:
        """
        Convert YYYY-MM-DD string to date

        :param str_date: string date
        :return: date
        """
        return datetime.strptime(str_date, "%Y-%m-%d").date()

    @staticmethod
    def date_subtract(run_date: date, units: str, value: int) -> date:
        """
        Generate starting date from given date and config

        :param run_date: base date
        :param units: units: years, months, weeks, days
        :param value: number of units to subtract
        :return: Starting date
        """
        if units == "years":
            return run_date - relativedelta(years=value)
        if units == "months":
            return run_date - relativedelta(months=value)
        if units == "weeks":
            return run_date - relativedelta(weeks=value)
        if units == "days":
            return run_date - relativedelta(days=value)
        raise ValueError(f"Unknown time unit {units}")

    @staticmethod
    def all_dates(date_from: date, date_to: date) -> List[date]:
        """
        Get list of all dates between, inclusive

        :param date_from: starting date
        :param date_to: ending date
        :return: List[date]
        """
        if date_to < date_from:
            date_to, date_from = date_from, date_to

        return [date_from + relativedelta(days=n) for n in range((date_to - date_from).days + 1)]

    @staticmethod
    def run_dates(date_from: date, date_to: date, schedule: ScheduleConfig) -> List[date]:
        """
        Select dates inside given interval depending on frequency and selected day

        :param date_from: interval start
        :param date_to: interval end
        :param schedule: schedule config
        :return: list of dates
        """
        options = DateManager.all_dates(date_from, date_to)
        if schedule.frequency == "daily":
            return options
        if schedule.frequency == "weekly":
            return [x for x in options if x.isoweekday() == schedule.day]
        if schedule.frequency == "monthly":
            return [x for x in options if x.day == schedule.day]
        raise ValueError(f"Unknown frequency {schedule.frequency}")

    @staticmethod
    def to_info_date(date: date, schedule: ScheduleConfig) -> date:
        """
        Shift given date according to config

        :param date: input date
        :param schedule: schedule config
        :return: date
        """
        if isinstance(schedule.info_date_shift, List):
            for shift in schedule.info_date_shift:
                date = DateManager.date_subtract(date, units=shift.units, value=shift.value)
        else:
            date = DateManager.date_subtract(
                date, units=schedule.info_date_shift.units, value=schedule.info_date_shift.value
            )
        return date
