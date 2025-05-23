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

import datetime
from datetime import date
from typing import Dict, List, Tuple

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

import rialto.runner.utils as utils
from rialto.common import TableReader
from rialto.runner.config_loader import PipelineConfig, get_pipelines_config
from rialto.runner.date_manager import DateManager
from rialto.runner.reporting.record import Record
from rialto.runner.reporting.tracker import Tracker
from rialto.runner.table import Table
from rialto.runner.transformation import Transformation


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
    ):
        self.spark = spark
        self.config = get_pipelines_config(config_path, overrides)
        self.reader = TableReader(spark)
        self.rerun = rerun
        self.skip_dependencies = skip_dependencies
        self.op = op
        self.tracker = Tracker(
            mail_cfg=self.config.runner.mail, bookkeeping=self.config.runner.bookkeeping, spark=spark
        )

        if run_date:
            run_date = DateManager.str_to_date(run_date)
        else:
            run_date = date.today()

        self.date_from = DateManager.date_subtract(
            run_date=run_date,
            units=self.config.runner.watched_period_units,
            value=self.config.runner.watched_period_value,
        )

        self.date_until = run_date

        if self.date_from > self.date_until:
            raise ValueError(f"Invalid date range from {self.date_from} until {self.date_until}")
        logger.info(f"Running period set to: {self.date_from} - {self.date_until}")

    def _execute(self, instance: Transformation, run_date: date, pipeline: PipelineConfig) -> DataFrame:
        """
        Run the job

        :param instance: Instance of Transformation
        :param run_date: date to run for
        :param pipeline: pipeline configuration
        :return: Dataframe
        """
        metadata_manager, feature_loader = utils.init_tools(self.spark, pipeline)

        df = instance.run(
            spark=self.spark,
            run_date=run_date,
            config=pipeline,
            reader=self.reader,
            metadata_manager=metadata_manager,
            feature_loader=feature_loader,
        )
        logger.info(f"Generated {df.count()} records")

        return df

    def _write(self, df: DataFrame, info_date: date, table: Table) -> None:
        """
        Write dataframe to storage

        :param df: dataframe to write
        :param info_date: date to partition
        :param table: path to write to
        :return: None
        """
        df = df.withColumn(table.partition, F.lit(info_date))
        df.write.partitionBy(table.partition).mode("overwrite").saveAsTable(table.get_table_path())
        logger.info(f"Results writen to {table.get_table_path()}")

    def check_dates_have_partition(self, table: Table, dates: List[date]) -> List[bool]:
        """
        For given list of dates, check if there is a matching partition for each

        :param table: Table object
        :param dates: list of dates to check
        :return: list of bool
        """
        if utils.table_exists(self.spark, table.get_table_path()):
            partitions = utils.get_partitions(self.reader, table)
            return [(date in partitions) for date in dates]
        else:
            logger.info(f"Table {table.get_table_path()} doesn't exist!")
            return [False for _ in dates]

    def check_dependencies(self, pipeline: PipelineConfig, run_date: date) -> bool:
        """
        Check for all dependencies in config if they have available partitions

        :param pipeline: configuration
        :param run_date: run date
        :return: bool
        """
        logger.info(f"{pipeline.name} checking dependencies for {run_date}")

        error = ""

        for dependency in pipeline.dependencies:
            dep_from = DateManager.date_subtract(run_date, dependency.interval.units, dependency.interval.value)
            logger.info(f"Looking for {dependency.table} from {dep_from} until {run_date}")

            possible_dep_dates = DateManager.all_dates(dep_from, run_date)

            logger.debug(f"Date column for {dependency.table} is {dependency.date_col}")

            source = Table(table_path=dependency.table, partition=dependency.date_col)
            if True in self.check_dates_have_partition(source, possible_dep_dates):
                logger.info(f"Dependency for {dependency.table} from {dep_from} until {run_date} is fulfilled")
            else:
                msg = f"Missing dependency for {dependency.table} from {dep_from} until {run_date}"
                logger.info(msg)
                error = error + msg + "\n"

        if error != "":
            self.tracker.last_error = error
            return False

        return True

    def _get_completion(self, target: Table, info_dates: List[date]) -> List[bool]:
        """
        Check if model has run for given dates

        :param target_path: Table object
        :param info_dates: list of dates
        :return: bool list
        """
        if self.rerun:
            return [False for _ in info_dates]
        else:
            return self.check_dates_have_partition(target, info_dates)

    def _select_run_dates(self, pipeline: PipelineConfig, table: Table) -> Tuple[List, List]:
        """
        Select run dates and info dates based on completion

        :param pipeline: pipeline config
        :param table: table path
        :return: list of run dates and list of info dates
        """
        possible_run_dates = DateManager.run_dates(self.date_from, self.date_until, pipeline.schedule)
        possible_info_dates = [DateManager.to_info_date(x, pipeline.schedule) for x in possible_run_dates]
        current_state = self._get_completion(table, possible_info_dates)

        selection = [
            (run, info) for run, info, state in zip(possible_run_dates, possible_info_dates, current_state) if not state
        ]

        if not len(selection):
            logger.info(f"{pipeline.name} has no dates to run")
            return [], []

        selected_run_dates, selected_info_dates = zip(*selection)
        logger.info(f"{pipeline.name} identified to run for {selected_run_dates}")

        return list(selected_run_dates), list(selected_info_dates)

    def _run_one_date(self, pipeline: PipelineConfig, run_date: date, info_date: date, target: Table) -> int:
        """
        Run one pipeline for one date

        :param pipeline: pipeline cfg
        :param run_date: run date
        :param info_date: information date
        :param target: target Table
        :return: success bool
        """
        if self.skip_dependencies or self.check_dependencies(pipeline, run_date):
            logger.info(f"Running {pipeline.name} for {run_date}")

            feature_group = utils.load_module(pipeline.module)
            df = self._execute(feature_group, run_date, pipeline)
            records = df.count()
            if records > 0:
                self._write(df, info_date, target)
                return records
            else:
                raise RuntimeError("No records generated")
        return 0

    def _run_pipeline(self, pipeline: PipelineConfig):
        """
        Run single pipeline for all required dates

        :param pipeline: pipeline cfg
        :return: success bool
        """
        target = Table(
            schema_path=pipeline.target.target_schema,
            class_name=pipeline.module.python_class,
            partition=pipeline.target.target_partition_column,
        )
        logger.info(f"Loaded pipeline {pipeline.name}")

        selected_run_dates, selected_info_dates = self._select_run_dates(pipeline, target)

        # ----------- Checking dependencies available ----------
        for run_date, info_date in zip(selected_run_dates, selected_info_dates):
            run_start = datetime.datetime.now()
            try:
                records = self._run_one_date(pipeline, run_date, info_date, target)
                if records > 0:
                    status = "Success"
                    message = ""
                else:
                    status = "Failure"
                    message = self.tracker.last_error
                self.tracker.add(
                    Record(
                        job=pipeline.name,
                        target=target.get_table_path(),
                        date=info_date,
                        time=datetime.datetime.now() - run_start,
                        records=records,
                        status=status,
                        reason=message,
                    )
                )
            except Exception as error:
                logger.error(f"An exception occurred in pipeline {pipeline.name}")
                logger.exception(error)
                self.tracker.add(
                    Record(
                        job=pipeline.name,
                        target=target.get_table_path(),
                        date=info_date,
                        time=datetime.datetime.now() - run_start,
                        records=0,
                        status="Error",
                        reason="Exception",
                        exception=str(error),
                    )
                )
            except KeyboardInterrupt:
                logger.error(f"Pipeline {pipeline.name} interrupted")
                self.tracker.add(
                    Record(
                        job=pipeline.name,
                        target=target.get_table_path(),
                        date=info_date,
                        time=datetime.datetime.now() - run_start,
                        records=0,
                        status="Error",
                        reason="Interrupted by user",
                    )
                )
                raise KeyboardInterrupt

    def __call__(self):
        """Execute pipelines"""
        logger.info("Executing pipelines")
        try:
            if self.op:
                selected = [p for p in self.config.pipelines if p.name == self.op]
                if len(selected) < 1:
                    raise ValueError(f"Unknown operation selected: {self.op}")
                self._run_pipeline(selected[0])
            else:
                for pipeline in self.config.pipelines:
                    self._run_pipeline(pipeline)
        finally:
            print(self.tracker.records)
            self.tracker.report_by_mail()
            logger.info("Execution finished")
