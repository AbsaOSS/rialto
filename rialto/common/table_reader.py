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

__all__ = ["DataReader", "TableReader"]

import abc
import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


class DataReader(metaclass=abc.ABCMeta):
    """
    This is an abstract class defining interface for reader of spark tables

    Data reader provides to public functions, get_latest and get_table.
    get_latest reads a single snapshot of the given table, while get_table reads the whole table or multiple snapshots.
    """

    @abc.abstractmethod
    def get_latest(
        self,
        table: str,
        date_column: str,
        date_until: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """
        Get latest available date partition of the table until specified date

        :param table: input table path
        :param date_until: Optional until date (inclusive)
        :param uppercase_columns: Option to refactor all column names to uppercase
        :return: Dataframe
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_table(
        self,
        table: str,
        date_column: str,
        date_from: Optional[datetime.date] = None,
        date_to: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """
        Get a whole table or a slice by selected dates

        :param table: input table path
        :param date_from: Optional date from (inclusive)
        :param date_to: Optional date to (inclusive)
        :param uppercase_columns: Option to refactor all column names to uppercase
        :return: Dataframe
        """
        raise NotImplementedError


class TableReader(DataReader):
    """An implementation of data reader for databricks tables"""

    def __init__(self, spark: SparkSession):
        """
        Init

        :param spark:
        """
        self.spark = spark
        super().__init__()

    def _uppercase_column_names(self, df: DataFrame) -> DataFrame:
        """
        Change the case of all column names to uppercase

        :param df: Dataframe
        :return: renamed Dataframe
        """
        for col in df.columns:
            df = df.withColumnRenamed(col, col.upper())
        return df

    def _get_latest_available_date(self, df: DataFrame, date_col: str, until: Optional[datetime.date]) -> datetime.date:
        if until:
            df = df.filter(F.col(date_col) <= until)
        df = df.select(F.max(date_col)).alias("latest")
        return df.head()[0]

    def get_latest(
        self,
        table: str,
        date_column: str,
        date_until: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """
        Get latest available date partition of the table until specified date

        :param table: input table path
        :param date_until: Optional until date (inclusive)
        :param date_column: column to filter dates on, takes highest priority
        :param uppercase_columns: Option to refactor all column names to uppercase
        :return: Dataframe
        """
        df = self.spark.read.table(table)

        selected_date = self._get_latest_available_date(df, date_column, date_until)
        df = df.filter(F.col(date_column) == selected_date)

        if uppercase_columns:
            df = self._uppercase_column_names(df)
        return df

    def get_table(
        self,
        table: str,
        date_column: str,
        date_from: Optional[datetime.date] = None,
        date_to: Optional[datetime.date] = None,
        uppercase_columns: bool = False,
    ) -> DataFrame:
        """
        Get a whole table or a slice by selected dates

        :param table: input table path
        :param date_from: Optional date from (inclusive)
        :param date_to: Optional date to (inclusive)
        :param date_column: column to filter dates on, takes highest priority
        :param uppercase_columns: Option to refactor all column names to uppercase
        :return: Dataframe
        """
        df = self.spark.read.table(table)

        if date_from:
            df = df.filter(F.col(date_column) >= date_from)
        if date_to:
            df = df.filter(F.col(date_column) <= date_to)
        if uppercase_columns:
            df = self._uppercase_column_names(df)
        return df
