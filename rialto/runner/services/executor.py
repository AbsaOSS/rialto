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

__all__ = ["PipelineExecutor"]

from importlib import import_module
from typing import Tuple

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from rialto.common import DataReader
from rialto.loader import PysparkFeatureLoader
from rialto.metadata import MetadataManager
from rialto.runner.services.config_loader import ModuleConfig, PipelineConfig
from rialto.runner.services.data_checker import DataChecker
from rialto.runner.services.task_registry import PipelineTask
from rialto.runner.transformation import Transformation


class PipelineExecutor:
    """Executes a single pipeline task."""

    def __init__(self, spark: SparkSession, reader: DataReader, checker: DataChecker):
        self.spark = spark
        self.reader = reader
        self.checker = checker

    def _init_tools(
        self, spark: SparkSession, pipeline: PipelineConfig
    ) -> Tuple[MetadataManager, PysparkFeatureLoader]:
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

    def _load_module(self, cfg: ModuleConfig) -> Transformation:
        """
        Load feature group

        :param cfg: Feature configuration
        :return: Transformation object
        """
        module = import_module(cfg.python_module)
        class_obj = getattr(module, cfg.python_class)
        return class_obj()

    def execute(self, pipeline: PipelineTask) -> DataFrame:
        """
        Execute the pipeline task.

        :param pipeline: Pipeline object to execute.
        :return: DataFrame resulting from pipeline execution.
        """
        logger.info(f"Executing pipeline {pipeline.op} for partition date {pipeline.partition_date}")

        # Load and run the job
        job = self._load_module(pipeline.config.module)
        metadata_manager, feature_loader = self._init_tools(self.spark, pipeline.config)
        df = job.run(
            spark=self.spark,
            run_date=pipeline.execution_date,
            config=pipeline.config,
            reader=self.reader,
            metadata_manager=metadata_manager,
            feature_loader=feature_loader,
        )
        return df
