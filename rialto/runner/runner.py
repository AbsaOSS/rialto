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

__all__ = ["Runner"]

from datetime import datetime
from typing import Dict, List

from execution_planner import ExecutionPlanner
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

import rialto.runner.utils as utils
from rialto.common import TableReader
from rialto.runner.config_loader import ConfigLoader, PipelineConfig
from rialto.runner.data_checker import DataChecker
from rialto.runner.date_manager import DateManager
from rialto.runner.reporting.record import Record
from rialto.runner.reporting.tracker import Tracker
from rialto.runner.writer import DatabricksWriter


class Runner:
    """A scheduler and dependency checker for feature runs"""

    def __init__(
        self,
        spark: SparkSession,
        config_path: str,
        run_date: str = None,
        rerun: bool = False,
        op: str = None,
        skip_dependencies: bool = False,
        overrides: Dict = None,
        merge_schema: bool = False,
    ):
        self.spark = spark
        self.config = ConfigLoader().load_yaml(config_path, overrides)
        self.reader = TableReader(spark)
        self.rerun = rerun
        self.op = op
        self.skip_dependencies = skip_dependencies
        self.writer = DatabricksWriter(spark, merge_schema=merge_schema)
        self.checker = DataChecker(self.reader)
        self.tracker = Tracker(
            mail_cfg=self.config.runner.mail, bookkeeping=self.config.runner.bookkeeping, spark=spark
        )
        self.date_manager = DateManager(self.config, run_date)
        self.planner = ExecutionPlanner()

    def _select_pipelines(self) -> List[PipelineConfig]:
        """Select pipelines to run based on config and input parameters"""
        if self.op:
            selected = [p for p in self.config.pipelines if p.name == self.op]
            if len(selected) < 1:
                raise ValueError(f"Unknown operation selected: {self.op}")
            return selected
        else:
            return self.config.pipelines

    def __call__(self):
        """Execute pipelines"""
        pipelines = self._select_pipelines()

        # Register pipelines in execution planner with their execution and partition dates
        for pipeline in pipelines:
            exec_date, partition_date = self.date_manager.get_execution_and_partition_dates(pipeline.schedule)
            self.planner.add_pipeline(
                name=pipeline.name, execution_date=exec_date, partition_date=partition_date, config=pipeline
            )

        for task in self.planner.tasks:
            task.check_completion(self.checker, self.rerun)
            task.check_dependencies_complete(self.checker, self.skip_dependencies)

        self.planner.log_status()

        # TODO everything bellow is just temporary
        for task in self.planner.tasks:
            if not task.completion and task.dependencies_complete:
                logger.info(f"Running pipeline {task.op} for partition date {task.partition_date}")
                job = utils.load_module(task.config.module)
                metadata_manager, feature_loader = utils.init_tools(self.spark, task.config)
                run_start = datetime.now()
                df = job.run(
                    spark=self.spark,
                    run_date=task.execution_date,
                    config=task.config,
                    reader=self.reader,
                    metadata_manager=metadata_manager,
                    feature_loader=feature_loader,
                )
                self.writer.write(df, task.partition_date, task.target)
                records = self.checker.check_written(task.target, task.package, df)

                self.tracker.add(
                    Record(
                        job=task.op,
                        target=task.target.get_table_path(),
                        date=task.partition_date,
                        time=datetime.now() - run_start,
                        records=records,
                        status="status",
                        reason="message",
                    )
                )

        # 6. run the pipeline for dates with completed dependencies
        # 7. write results
        # 8. sumbit tracking

    def debug(self) -> DataFrame:
        """Debug mode - run only first op for one date and return the resulting dataframe"""
