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

__all__ = ["DatabricksWriter", "Writer"]

from abc import ABC, abstractmethod
from datetime import date
from typing import List

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from rialto.runner.table import Table


class Writer(ABC):
    """Supporting class for runner"""

    @abstractmethod
    def write(self, df: DataFrame, info_date: date, table: Table) -> None:
        """
        Write dataframe to storage

        :param df: dataframe to write
        :param info_date: date to partition
        :param table: path to write to
        :return: None
        """
        pass


class DatabricksWriter(Writer):
    """Supporting class for runner, Databricks write operations"""

    def __init__(self, spark: SparkSession, merge_schema=False):
        self.spark = spark
        self.merge_schema = merge_schema

    def _create_schema(self, table: Table):
        """
        Create schema if it doesn't exist

        :param schema_path: path to schema
        """
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table.get_schema_path()}")

    def _get_existing_columns(self, table: Table):
        """
        Get existing schema of table if it exists

        :param table: table to check for
        :return: existing columns or None
        """
        try:
            return self.spark.table(table.get_table_path()).columns
        except Exception as e:
            logger.warning(f"Could not get existing schema for {table.get_table_path()}: {e}")
            return None

    def _align_schema(self, df: DataFrame, existing_columns: List) -> DataFrame:
        """
        Align schema of dataframe to existing schema of table if it exists

        :param df: dataframe to align
        :param table: table to check for existing schema
        :return: dataframe with aligned schema
        """
        if existing_columns is not None:
            return df.select(
                *[F.col(c) for c in existing_columns if c in df.columns],
                *[F.col(c) for c in df.columns if c not in existing_columns],
            )
        return df

    def _process(self, df: DataFrame, info_date: date, table: Table) -> DataFrame:
        df = df.withColumn(table.partition, F.lit(info_date))

        df = self._align_schema(df, self._get_existing_columns(table))

        return df

    def _get_replace_condition(self, df: DataFrame, partition_cols: List[str]) -> str:
        row = df.select(*partition_cols).distinct().collect()
        if len(row) > 1:
            raise ValueError(f"Some of the partitions to write have more than 1 distinct value \n {row}")

        parts = []
        for c in partition_cols:
            val = row[0][c]
            if val is None:
                parts.append(f"{c} IS NULL")
            elif isinstance(val, (int, float)):
                parts.append(f"{c} = {val}")
            else:
                parts.append(f"{c} = '{val}'")
        condition = " AND ".join(parts)
        return condition

    def write(self, df: DataFrame, info_date: date, table: Table) -> None:
        """
        Write dataframe to storage

        :param df: dataframe to write
        :param info_date: date to partition
        :param table: path to write to
        :return: None
        """
        self._create_schema(table)

        df = self._process(df, info_date, table)

        replace_where = self._get_replace_condition(df, table.get_all_partition_columns())

        df.write.format("delta").partitionBy(table.partition).mode("overwrite").option(
            "mergeSchema", "true" if self.merge_schema else "false"
        ).option("replaceWhere", replace_where).saveAsTable(table.get_table_path())

        logger.info(f"Results writen to {table.get_table_path()}")
