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
from loguru import logger

from rialto.runner.config_loader import RunnerConfig, ScheduleConfig


class DateManager:
    """Date generation and shifts based on configuration"""

    def __init__(self, config: RunnerConfig, run_date: str = None):
        if run_date:
            run_date = self.str_to_date(run_date)
        else:
            run_date = date.today()

        self.date_from = self.date_subtract(
            input_date=run_date,
            units=config.watched_period_units,
            value=config.watched_period_value,
        )

        self.date_until = run_date

        if self.date_from > self.date_until:
            raise ValueError(f"Invalid date range from {self.date_from} until {self.date_until}")
        logger.info(f"Running period set to: {self.date_from} - {self.date_until}")

    def get_date_from(self) -> date:
        """Get starting date of the execution window"""
        return self.date_from

    def get_date_until(self) -> date:
        """Get ending date of the execution window"""
        return self.date_until

    @staticmethod
    def str_to_date(str_date: str) -> date:
        """
        Convert YYYY-MM-DD string to date

        :param str_date: string date
        :return: date
        """
        try:
            return datetime.strptime(str_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format: {str_date}. Expected YYYY-MM-DD.")

    @staticmethod
    def date_subtract(input_date: date, units: str, value: int) -> date:
        """
        Subtract given number of units from input date

        :param input_date: base date
        :param units: units: years, months, weeks, days
        :param value: number of units to subtract
        :return: Starting date
        """
        if units == "years":
            return input_date - relativedelta(years=value)
        if units == "months":
            return input_date - relativedelta(months=value)
        if units == "weeks":
            return input_date - relativedelta(weeks=value)
        if units == "days":
            return input_date - relativedelta(days=value)
        raise ValueError(f"Unknown time unit {units}")

    @staticmethod
    def all_dates(date_from: date, date_until: date) -> List[date]:
        """
        Get list of all dates between, inclusive

        :param date_from: starting date
        :param date_until: ending date
        :return: List[date]
        """
        return [date_from + relativedelta(days=n) for n in range((date_until - date_from).days + 1)]

    def get_execution_and_partition_dates(self, schedule: ScheduleConfig) -> List[tuple[date, date]]:
        """
        Get list of execution and partition dates for given configuration

        :return: List of tuples with execution and partition dates
        """
        execution_dates = self._execution_dates(schedule)
        return [(ex_date, self._to_partition_date(ex_date, schedule)) for ex_date in execution_dates]

    def _execution_dates(self, schedule: ScheduleConfig) -> List[date]:
        """
        Select dates inside given interval depending on frequency and selected day

        :param schedule: schedule config
        :return: List of execution dates
        """
        options = self.all_dates(self.date_from, self.date_until)
        frequency = schedule.frequency.lower()
        if frequency == "daily":
            return options
        if frequency == "weekly":
            if not (1 <= schedule.day <= 7):
                raise ValueError(f"Invalid day for weekly frequency: {schedule.day}. Must be 1-7.")
            return [x for x in options if x.isoweekday() == schedule.day]
        if frequency == "monthly":
            if schedule.day == "last":
                return [x for x in options if (x + relativedelta(days=1)).month != x.month]
            if not (1 <= schedule.day <= 31):
                raise ValueError(f"Invalid day for monthly frequency: {schedule.day}. Must be 1-31 or last.")
            return [x for x in options if x.day == schedule.day]
        raise ValueError(f"Unknown frequency: {schedule.frequency}")

    def _to_partition_date(self, date: date, schedule: ScheduleConfig) -> date:
        """
        Shift given date according to config

        :param date: input date
        :param schedule: schedule config
        :return: date
        """
        if isinstance(schedule.info_date_shift, list):
            for shift in schedule.info_date_shift:
                date = self.date_subtract(date, units=shift.units, value=shift.value)
        else:
            date = self.date_subtract(date, units=schedule.info_date_shift.units, value=schedule.info_date_shift.value)
        return date
