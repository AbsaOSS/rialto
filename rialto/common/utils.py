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

__all__ = ["load_yaml"]

import os
from typing import Any

import pyspark.sql.functions as F
import yaml
from env_yaml import EnvLoader
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType


def load_yaml(path: str) -> Any:
    """
    YAML loader

    :param path: file path
    :return: Parsed yaml
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Can't find {path}.")

    with open(path, "r") as stream:
        return yaml.load(stream, EnvLoader)


def get_date_col_property(spark, table: str, property: str) -> str:
    """
    Retrieve a data column name from a given table property

    :param spark: spark session
    :param table: path to table
    :param property: name of the property
    :return: data column name
    """
    props = spark.sql(f"show tblproperties {table}")
    date_col = props.filter(F.col("key") == property).select("value").collect()
    if len(date_col):
        return date_col[0].value
    else:
        raise RuntimeError(f"Table {table} has no property {property}.")


def get_delta_partition(spark, table: str) -> str:
    """
    Select first partition column of the delta table

    :param table: full table name
    :return: partition column name
    """
    columns = spark.catalog.listColumns(table)
    partition_columns = list(filter(lambda c: c.isPartition, columns))
    if len(partition_columns):
        return partition_columns[0].name
    else:
        raise RuntimeError(f"Delta table has no partitions: {table}.")


def cast_decimals_to_floats(df: DataFrame) -> DataFrame:
    """
    Find all decimal types in the table and cast them to floats. Fixes errors in .toPandas() conversions.

    :param df: pyspark DataFrame
    :return: pyspark DataFrame with fixed types
    """
    decimal_cols = [col_name for col_name, data_type in df.dtypes if "decimal" in data_type]
    for c in decimal_cols:
        df = df.withColumn(c, F.col(c).cast(FloatType()))

    return df
