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
from datetime import datetime

from loguru import logger
from pyspark.sql import SparkSession

import rialto.runner.utils as utils
from rialto.common import DataReader
from rialto.runner.data_checker import DataChecker
from rialto.runner.execution_planner import Task
from rialto.runner.reporting.record import Record
from rialto.runner.reporting.tracker import Tracker
from rialto.runner.writer import Writer


class PipelineExecutor:
    """Executes a single pipeline task."""

    def __init__(self, spark: SparkSession, reader: DataReader, writer: Writer, checker: DataChecker, tracker: Tracker):
        self.spark = spark
        self.reader = reader
        self.writer = writer
        self.checker = checker
        self.tracker = tracker

    def execute(self, pipeline: Task):
        """
        Execute the pipeline task.

        :param pipeline: Pipeline object to execute.
        :return: None
        """
        logger.info(f"Executing pipeline {pipeline.op} for partition date {pipeline.partition_date}")
        run_start = datetime.now()

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

        # Write the output
        self.writer.write(df, pipeline.partition_date, pipeline.target)

        # Perform checks and track results
        records = self.checker.check_written(pipeline.target, pipeline.partition_date, df)
        self.tracker.add(
            Record(
                job=pipeline.op,
                target=pipeline.target.get_table_path(),
                date=pipeline.partition_date,
                time=datetime.now() - run_start,
                records=records,
                status="status",
                reason="message",
            )
        )
