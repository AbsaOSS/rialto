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

__all__ = ["DatabricksLoader"]

from datetime import date

from pyspark.sql import DataFrame, SparkSession

from rialto.common.table_reader import TableReader
from rialto.loader.interfaces import DataLoader


class DatabricksLoader(DataLoader):
    """Implementation of DataLoader using TableReader to access feature tables"""

    def __init__(self, spark: SparkSession, schema: str, date_column: str = "INFORMATION_DATE"):
        super().__init__()

        self.reader = TableReader(spark)
        self.schema = schema
        self.date_col = date_column

    def read_group(self, group: str, information_date: date) -> DataFrame:
        """
        Read a feature group by getting the latest partition by date

        :param group: group name
        :param information_date: partition date
        :return: dataframe
        """
        return self.reader.get_latest(
            f"{self.schema}.{group}", date_until=information_date, date_column=self.date_col, uppercase_columns=True
        )
