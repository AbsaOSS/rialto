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

import traceback
from datetime import datetime
from typing import Dict, List

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from rialto.common import TableReader
from rialto.runner.config_loader import ConfigLoader, PipelineConfig
from rialto.runner.data_checker import DataChecker
from rialto.runner.date_manager import DateManager
from rialto.runner.executor import PipelineExecutor
from rialto.runner.reporting.record import Record
from rialto.runner.reporting.tracker import Tracker
from rialto.runner.task_registry import PipelineTask, TaskRegistry
from rialto.runner.task_status_checker import TaskStatusChecker
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
        self.config = ConfigLoader().load_yaml(config_path, overrides)
        self.date_manager = DateManager(self.config.runner, run_date)
        self.rerun = rerun
        self.op = op
        self.skip_dependencies = skip_dependencies
        self.writer = DatabricksWriter(spark, merge_schema=merge_schema)

        reader = TableReader(spark)
        self.data_checker = DataChecker(reader)
        self.task_checker = TaskStatusChecker(self.data_checker)
        self.registry = TaskRegistry(spark, date_manager=self.date_manager)
        self.executor = PipelineExecutor(
            spark=spark,
            reader=reader,
            checker=self.data_checker,
        )
        self.tracker = Tracker(
            mail_cfg=self.config.runner.mail, bookkeeping=self.config.runner.bookkeeping, spark=spark
        )

    def _select_pipelines(self) -> List[PipelineConfig]:
        """Select pipelines to run based on config and input parameters"""
        if self.op:
            selected = [p for p in self.config.pipelines if p.name == self.op]
            if len(selected) < 1:
                raise ValueError(f"Unknown operation selected: {self.op}")
            return selected
        else:
            return self.config.pipelines

    def _register_tasks(self, pipelines: List[PipelineConfig]) -> None:
        for pipeline in pipelines:
            for exec_date, partition_date in self.date_manager.get_execution_and_partition_dates(pipeline.schedule):
                self.registry.add_task(
                    name=pipeline.name, execution_date=exec_date, partition_date=partition_date, config=pipeline
                )

    def _check_tasks(self) -> None:
        for task in self.registry.tasks:
            if not self.rerun:
                self.task_checker.check_completion(task)
            if not self.skip_dependencies:
                self.task_checker.check_pipeline_dependencies(task)

    def _get_dependency_failure_reason(self, task: PipelineTask) -> str:
        failed = [
            f"{dep.table.get_table_path()} from {dep.date_from} until {dep.date_until}"
            for dep in task.dependencies
            if not dep.complete
        ]
        return "Incomplete dependencies:\n" + ",\n".join(failed) if failed else "No dependency failures"

    def _make_record(
        self, task: PipelineTask, run_start: datetime, status: str, reason: str, records: int = 0, exception: str = None
    ) -> Record:
        return Record(
            job=task.op,
            target=task.target.get_table_path(),
            date=task.partition_date,
            time=datetime.now() - run_start,
            records=records,
            status=status,
            reason=reason,
            exception=exception,
        )

    def _run_tasks(self) -> None:
        for task in self.registry.tasks:
            run_start = datetime.now()

            if not task.completion or self.rerun:
                if not (task.dependencies_complete or self.skip_dependencies):
                    self.tracker.add(
                        self._make_record(task, run_start, "Failure", self._get_dependency_failure_reason(task))
                    )
                    continue
                try:
                    df = self.executor.execute(task)
                    self.writer.write(df, task.partition_date, task.target)
                    records = self.data_checker.check_written(task.target, task.partition_date, df)
                    self.tracker.add(self._make_record(task, run_start, "Success", "OK", records=records))
                except KeyboardInterrupt:
                    self.tracker.add(self._make_record(task, run_start, "Error", "Interrupted by user"))
                    raise
                except Exception as e:
                    logger.exception(e)
                    self.tracker.add(
                        self._make_record(task, run_start, "Error", type(e).__name__, exception=traceback.format_exc())
                    )

    def __call__(self):
        """Execute pipelines"""
        pipelines = self._select_pipelines()
        self._register_tasks(pipelines)
        self._check_tasks()
        self.registry.log_status()
        self._run_tasks()
        self.tracker.report_by_mail()

    def dry_run(self):
        """Dry run - log status of pipelines without executing"""
        pipelines = self._select_pipelines()
        self._register_tasks(pipelines)
        self._check_tasks()
        self.registry.log_status()

    def debug(self) -> DataFrame:
        """Debug mode - run only first op for one date and return the resulting dataframe"""
        pipelines = self._select_pipelines()
        self._register_tasks(pipelines)
        return self.executor.execute(self.registry.tasks[0])
