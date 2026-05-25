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
__all__ = ["DataChecker"]

from datetime import date
from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame

from rialto.common import DataReader
from rialto.runner.services.table import Table


class DataChecker:
    """Checks if data for given date or date range is present in storage"""

    def __init__(self, reader: DataReader):
        self.reader = reader

    def check_date(self, target: Table, partition_date: date) -> bool:
        """Check if data for given date is present in target

        :param target: target Table to check
        :param partition_date: Date to check
        :return: True if data for given date is present, False otherwise
        """
        return self.check_range(target, partition_date, partition_date)

    def check_range(self, target: Table, start_date: date, end_date: date) -> bool:
        """Check if data for given date range is present in target

        :param target: target Table to check
        :param start_date: Starting date of the range to check
        :param end_date: Ending date of the range to check
        :return: True if data for given date range is present, False otherwise
        """
        if self.reader.table_exists(target.get_table_path()):
            df = self.reader.get_table(
                target.get_table_path(),
                date_column=target.partition,
                date_from=start_date,
                date_to=end_date,
                filters=target.filters,
            )
            data_exists = df.count() > 0
            if (
                data_exists
                and (target.filters is None or target.filters == {})
                and target.secondary_partitions is not None
            ):
                logger.warning(
                    f"Overwriting {target.get_table_path()} completion status for {start_date} due to presence of "
                    f"secondary partitions and no filters."
                )
                data_exists = False
            return data_exists
        else:
            logger.warning(f"Target table {target.get_table_path()} doesn't exist yet.")
            return False

    def _get_filters(self, target: Table, df: DataFrame) -> Dict:
        if target.filters is not None:
            return target.filters
        elif target.secondary_partitions:
            filters = {}
            logger.info("Inferring target sub-partition values from generated data.")
            row = df.select(*target.secondary_partitions).distinct().collect()[0]
            for c in target.secondary_partitions:
                filters[c] = row[c]
            return filters
        else:
            return {}

    def check_written(self, target: Table, partition_date: date, df: DataFrame) -> int:
        """Check how many records were written

        :param target: target Table to check
        :param partition_date: Date to check
        :param df: DataFrame that was written, used to determine filters if not provided in config
        :return: Number of records for given date
        """
        filters = self._get_filters(target, df)
        df = self.reader.get_table(
            target.get_table_path(),
            date_column=target.partition,
            date_from=partition_date,
            date_to=partition_date,
            filters=filters,
        )

        return df.count()
