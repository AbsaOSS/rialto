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

from rialto.runner.config_loader import PipelinesConfig, ScheduleConfig


class DateManager:
    """Date generation and shifts based on configuration"""

    def __init__(self, config: PipelinesConfig, run_date: date = None):
        self.config = config
        if run_date:
            run_date = self.str_to_date(run_date)
        else:
            run_date = date.today()

        self.date_from = self.date_subtract(
            run_date=run_date,
            units=self.config.runner.watched_period_units,
            value=self.config.runner.watched_period_value,
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

    def get_execution_and_partition_dates(self, schedule: ScheduleConfig) -> List[tuple[date, date]]:
        """
        Get list of execution and partition dates for given configuration

        :return: List of tuples with execution and partition dates
        """
        datepairs = []
        execution = self.execution_dates(schedule)
        for ex_date in execution:
            partition = self.to_partition_date(ex_date, schedule)
            datepairs.append((ex_date, partition))
        return datepairs

    def str_to_date(self, str_date: str) -> date:
        """
        Convert YYYY-MM-DD string to date

        :param str_date: string date
        :return: date
        """
        return datetime.strptime(str_date, "%Y-%m-%d").date()

    def date_subtract(self, run_date: date, units: str, value: int) -> date:
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

    def all_dates(self, date_from: date, date_to: date) -> List[date]:
        """
        Get list of all dates between, inclusive

        :param date_from: starting date
        :param date_to: ending date
        :return: List[date]
        """
        if date_to < date_from:
            date_to, date_from = date_from, date_to

        return [date_from + relativedelta(days=n) for n in range((date_to - date_from).days + 1)]

    def execution_dates(self, schedule: ScheduleConfig) -> List[date]:
        """
        Select dates inside given interval depending on frequency and selected day

        :param schedule: schedule config
        :return: list of dates
        """
        options = self.all_dates(self.date_from, self.date_until)
        if schedule.frequency == "daily":
            return options
        if schedule.frequency == "weekly":
            return [x for x in options if x.isoweekday() == schedule.day]
        if schedule.frequency == "monthly":
            return [x for x in options if x.day == schedule.day]
        raise ValueError(f"Unknown frequency {schedule.frequency}")

    def to_partition_date(self, date: date, schedule: ScheduleConfig) -> date:
        """
        Shift given date according to config

        :param date: input date
        :param schedule: schedule config
        :return: date
        """
        if isinstance(schedule.info_date_shift, List):
            for shift in schedule.info_date_shift:
                date = self.date_subtract(date, units=shift.units, value=shift.value)
        else:
            date = self.date_subtract(date, units=schedule.info_date_shift.units, value=schedule.info_date_shift.value)
        return date
