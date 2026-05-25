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

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

import rialto.runner.utils as utils
from rialto.common import DataReader
from rialto.runner.services.data_checker import DataChecker
from rialto.runner.services.task_registry import PipelineTask


class PipelineExecutor:
    """Executes a single pipeline task."""

    def __init__(self, spark: SparkSession, reader: DataReader, checker: DataChecker):
        self.spark = spark
        self.reader = reader
        self.checker = checker

    def execute(self, pipeline: PipelineTask) -> DataFrame:
        """
        Execute the pipeline task.

        :param pipeline: Pipeline object to execute.
        :return: DataFrame resulting from pipeline execution.
        """
        logger.info(f"Executing pipeline {pipeline.op} for partition date {pipeline.partition_date}")

        # Load and run the job
        job = utils.load_module(pipeline.config.module)
        metadata_manager, feature_loader = utils.init_tools(self.spark, pipeline.config)
        df = job.run(
            spark=self.spark,
            run_date=pipeline.execution_date,
            config=pipeline.config,
            reader=self.reader,
            metadata_manager=metadata_manager,
            feature_loader=feature_loader,
        )
        return df
