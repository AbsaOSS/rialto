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

__all__ = [
    "load_module",
    "table_exists",
    "get_available_dates",
    "init_tools",
    "find_dependency",
    "get_rows_from_history",
]

from datetime import date
from importlib import import_module
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from rialto.loader import PysparkFeatureLoader
from rialto.metadata import MetadataManager
from rialto.runner.config_loader import ModuleConfig, PipelineConfig
from rialto.runner.table import Table
from rialto.runner.transformation import Transformation


def load_module(cfg: ModuleConfig) -> Transformation:
    """
    Load feature group

    :param cfg: Feature configuration
    :return: Transformation object
    """
    module = import_module(cfg.python_module)
    class_obj = getattr(module, cfg.python_class)
    return class_obj()


def table_exists(spark: SparkSession, table: str) -> bool:
    """
    Check table exists in spark catalog

    :param table: full table path
    :return: bool
    """
    return spark.catalog.tableExists(table)


def get_available_dates(spark: SparkSession, table: Table, filters: Optional[Dict[str, str]] = None) -> List[date]:
    """
    Get distinct date values from table's partitions using SHOW PARTITIONS.

    :param spark: SparkSession instance
    :param table: Table object
    :param filters: Optional dict of partition column filters to apply
    :return: List of date values
    """
    partition_df = spark.sql(f"SHOW PARTITIONS {table.get_table_path()}")

    if table.date_column not in partition_df.columns:
        raise ValueError(
            f"date_column '{table.date_column}' not found in partitions of {table.get_table_path()}. "
            f"Available partition columns: {partition_df.columns}"
        )

    if filters:
        for col, val in filters.items():
            if col not in partition_df.columns:
                raise ValueError(
                    f"Filter column '{col}' not found in partitions of {table.get_table_path()}. "
                    f"Available partition columns: {partition_df.columns}"
                )
            partition_df = partition_df.filter(partition_df[col] == val)

    date_rows = partition_df.select(table.date_column).distinct().collect()
    return [row[table.date_column] for row in date_rows]


def init_tools(spark: SparkSession, pipeline: PipelineConfig) -> Tuple[MetadataManager, PysparkFeatureLoader]:
    """
    Initialize metadata manager and feature loader

    :param spark: Spark session
    :param pipeline: Pipeline configuration
    :return: MetadataManager and PysparkFeatureLoader
    """
    if pipeline.metadata_manager is not None:
        metadata_manager = MetadataManager(spark, pipeline.metadata_manager.metadata_schema)
    else:
        metadata_manager = None

    if pipeline.feature_loader is not None:
        feature_loader = PysparkFeatureLoader(
            spark,
            feature_schema=pipeline.feature_loader.feature_schema,
            metadata_schema=pipeline.feature_loader.metadata_schema,
        )
    else:
        feature_loader = None
    return metadata_manager, feature_loader


def find_dependency(config: PipelineConfig, name: str):
    """
    Get dependency from config

    :param config: Pipeline configuration
    :param name: Dependency name
    :return: Dependency object
    """
    for dep in config.dependencies:
        if dep.name == name:
            return dep
    return None


def get_rows_from_history(spark: SparkSession, table_path: str, version: str) -> int:
    """
    Get the number of rows written in a specific Delta commit version.

    :param spark: SparkSession instance
    :param table_path: Full table path (catalog.schema.table)
    :param version: The commit version to look up
    :return: Number of rows written, or 0 if unable to determine
    """
    history_df = spark.sql(f"DESCRIBE HISTORY {table_path}")
    version_row = history_df.filter(history_df.version == version).first()

    if version_row and version_row.operationMetrics.get("numOutputRows"):
        return int(version_row.operationMetrics.get("numOutputRows"))

    return 0
