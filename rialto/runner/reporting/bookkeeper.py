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
from pyspark.sql import DataFrame, SparkSession

from rialto.runner.reporting.record import Record


class BookKeeper:
    """Class to store and update records of runs in a table in the spark catalog."""

    def __init__(self, table: str, spark: SparkSession):
        self.spark = spark
        self.table = table

    def _write(self, df: DataFrame) -> None:
        if self.spark.catalog.tableExists(self.table):
            df.write.mode("append").saveAsTable(self.table)
        else:
            df.write.mode("overwrite").saveAsTable(self.table)

    def add(self, record: Record) -> None:
        """
        Add a record to the table.

        :param record: Record to add to the table.
        """
        new = self.spark.createDataFrame([record.to_spark_row()], record.get_schema())

        self._write(new)
