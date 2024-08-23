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
from importlib import import_module
from typing import List, Tuple

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from rialto.common import TableReader
from rialto.common.utils import get_date_col_property, get_delta_partition
from rialto.jobs.configuration.config_holder import ConfigHolder
from rialto.metadata import MetadataManager
from rialto.runner.config_loader import (
    DependencyConfig,
    ModuleConfig,
    PipelineConfig,
    ScheduleConfig,
    get_pipelines_config,
    transform_dependencies,
)
from rialto.runner.date_manager import DateManager
from rialto.runner.table import Table
from rialto.runner.tracker import Record, Tracker
from rialto.runner.transformation import Transformation


class Runner:
    """A scheduler and dependency checker for feature runs"""

    def __init__(
        self,
        spark: SparkSession,
        config_path: str,
        feature_metadata_schema: str = None,
        run_date: str = None,
        date_from: str = None,
        date_until: str = None,
        feature_store_schema: str = None,
        custom_job_config: dict = None,
        rerun: bool = False,
        op: str = None,
    ):
        self.spark = spark
        self.config = get_pipelines_config(config_path)
        self.reader = TableReader(
            spark, date_property=self.config.general.source_date_column_property, infer_partition=False
        )
        if feature_metadata_schema:
            self.metadata = MetadataManager(spark, feature_metadata_schema)
        else:
            self.metadata = None
        self.date_from = date_from
        self.date_until = date_until
        self.rerun = rerun
        self.op = op
        self.tracker = Tracker(self.config.general.target_schema)

        if (feature_store_schema is not None) and (feature_metadata_schema is not None):
            ConfigHolder.set_feature_store_config(feature_store_schema, feature_metadata_schema)

        if custom_job_config is not None:
            ConfigHolder.set_custom_config(**custom_job_config)

        if run_date:
            run_date = DateManager.str_to_date(run_date)
        else:
            run_date = date.today()
        if self.date_from:
            self.date_from = DateManager.str_to_date(date_from)
        if self.date_until:
            self.date_until = DateManager.str_to_date(date_until)

        if not self.date_from:
            self.date_from = DateManager.date_subtract(
                run_date=run_date,
                units=self.config.general.watched_period_units,
                value=self.config.general.watched_period_value,
            )
        if not self.date_until:
            self.date_until = run_date
        if self.date_from > self.date_until:
            raise ValueError(f"Invalid date range from {self.date_from} until {self.date_until}")
        logger.info(f"Running period from {self.date_from} until {self.date_until}")

    def _load_module(self, cfg: ModuleConfig) -> Transformation:
        """
        Load feature group

        :param cfg: Feature configuration
        :return: Transformation object
        """
        module = import_module(cfg.python_module)
        class_obj = getattr(module, cfg.python_class)
        return class_obj()

    def _generate(
        self, instance: Transformation, run_date: date, dependencies: List[DependencyConfig] = None
    ) -> DataFrame:
        """
        Run feature group

        :param instance: Instance of Transformation
        :param run_date: date to run for
        :return: Dataframe
        """
        if dependencies is not None:
            dependencies = transform_dependencies(dependencies)
        df = instance.run(
            reader=self.reader,
            run_date=run_date,
            spark=self.spark,
            metadata_manager=self.metadata,
            dependencies=dependencies,
        )
        logger.info(f"Generated {df.count()} records")

        return df

    def _table_exists(self, table: str) -> bool:
        """
        Check table exists in spark catalog

        :param table: full table path
        :return: bool
        """
        return self.spark.catalog.tableExists(table)

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

        try:
            get_date_col_property(self.spark, table.get_table_path(), "rialto_date_column")
        except RuntimeError:
            sql_query = (
                f"ALTER TABLE {table.get_table_path()} SET TBLPROPERTIES ('rialto_date_column' = '{table.partition}')"
            )
            self.spark.sql(sql_query)
            logger.info(f"Set table property rialto_date_column to {table.partition}")

    def _delta_partition(self, table: str) -> str:
        """
        Select first partition column, should be only one

        :param table: full table name
        :return: partition column name
        """
        columns = self.spark.catalog.listColumns(table)
        partition_columns = list(filter(lambda c: c.isPartition, columns))
        if len(partition_columns):
            return partition_columns[0].name
        else:
            raise RuntimeError(f"Delta table has no partitions: {table}.")

    def _get_partitions(self, table: Table) -> List[date]:
        """
        Get partition values

        :param table: Table object
        :return: List of partition values
        """
        rows = (
            self.reader.get_table(table.get_table_path(), date_column=table.partition)
            .select(table.partition)
            .distinct()
            .collect()
        )
        return [r[table.partition] for r in rows]

    def check_dates_have_partition(self, table: Table, dates: List[date]) -> List[bool]:
        """
        For given list of dates, check if there is a matching partition for each

        :param table: Table object
        :param dates: list of dates to check
        :return: list of bool
        """
        if self._table_exists(table.get_table_path()):
            partitions = self._get_partitions(table)
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

            # date column options prioritization (manual column, table property, inferred from delta)
            if dependency.date_col:
                date_col = dependency.date_col
            elif self.config.general.source_date_column_property:
                date_col = get_date_col_property(
                    self.spark, dependency.table, self.config.general.source_date_column_property
                )
            else:
                date_col = get_delta_partition(self.spark, dependency.table)
            logger.debug(f"Date column for {dependency.table} is {date_col}")

            source = Table(table_path=dependency.table, partition=date_col)
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

    def get_possible_run_dates(self, schedule: ScheduleConfig) -> List[date]:
        """
        List possible run dates according to parameters and config

        :param schedule: schedule config
        :return: List of dates
        """
        return DateManager.run_dates(self.date_from, self.date_until, schedule)

    def get_info_dates(self, schedule: ScheduleConfig, run_dates: List[date]) -> List[date]:
        """
        Transform given dates into info dates according to the config

        :param schedule: schedule config
        :param run_dates: date list
        :return: list of modified dates
        """
        return [DateManager.to_info_date(x, schedule) for x in run_dates]

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
        possible_run_dates = self.get_possible_run_dates(pipeline.schedule)
        possible_info_dates = self.get_info_dates(pipeline.schedule, possible_run_dates)
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
        if self.check_dependencies(pipeline, run_date):
            logger.info(f"Running {pipeline.name} for {run_date}")

            if self.config.general.job == "run":
                feature_group = self._load_module(pipeline.module)
                df = self._generate(feature_group, run_date, pipeline.dependencies)
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
            schema_path=self.config.general.target_schema,
            class_name=pipeline.module.python_class,
            partition=self.config.general.target_partition_column,
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
                print(f"An exception occurred in pipeline {pipeline.name}")
                print(error)
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
                print(f"Pipeline {pipeline.name} interrupted")
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
            self.tracker.report(self.config.general.mail)
