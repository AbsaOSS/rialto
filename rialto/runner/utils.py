from datetime import date
from importlib import import_module
from typing import List, Tuple

from pyspark.sql import SparkSession

from rialto.common import DataReader
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


def get_partitions(reader: DataReader, table: Table) -> List[date]:
    """
    Get partition values

    :param table: Table object
    :return: List of partition values
    """
    rows = (
        reader.get_table(table.get_table_path(), date_column=table.partition)
        .select(table.partition)
        .distinct()
        .collect()
    )
    return [r[table.partition] for r in rows]


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
