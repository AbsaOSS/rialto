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

from dataclasses import dataclass, field
from datetime import date
from typing import Iterator, List

from loguru import logger
from pyspark.sql import SparkSession

from rialto.runner.config_loader import PipelineConfig
from rialto.runner.data_checker import DataChecker
from rialto.runner.date_manager import DateManager
from rialto.runner.table import Table


@dataclass
class Dependency:
    """Class representing a pipeline dependency, with associated table and date range for checking completion"""

    table: Table
    date_from: date
    date_until: date
    complete: bool = False


@dataclass
class Task:
    """Class representing a pipeline to be executed."""

    op: str
    execution_date: date
    partition_date: date
    config: PipelineConfig
    target: Table
    dependencies: List[Dependency] = field(default_factory=list)
    completion: bool = False
    dependencies_complete: bool = False


class ExecutionPlanner:
    """Planner for pipeline execution, managing tasks and their dependencies"""

    def __init__(self, spark: SparkSession, date_manager: DateManager):
        self.spark = spark
        self.date_manager = date_manager
        self.tasks = []

    def add_task(self, name: str, execution_date: date, partition_date: date, config: PipelineConfig) -> None:
        """
        Add task to execution plan

        :param name: Name of the pipeline
        :param execution_date: Date when the pipeline is scheduled to run
        :param partition_date: Date for which the pipeline is processing data
        :param config: PipelineConfig object with pipeline configuration

        :return: None, adds a Pipeline object to self.tasks
        """
        target = Table.from_target_config(config)
        new_pipe = Task(
            op=name, execution_date=execution_date, partition_date=partition_date, config=config, target=target
        )

        for dependency_config in config.dependencies:
            dependency_table = Table.from_dependency_config(dependency_config)
            dependency_from = self.date_manager.date_subtract(
                execution_date, dependency_config.interval.units, dependency_config.interval.value
            )
            dependency = Dependency(table=dependency_table, date_from=dependency_from, date_until=execution_date)
            new_pipe.dependencies.append(dependency)

        self.tasks.append(new_pipe)

    def __iter__(self) -> Iterator[Task]:
        """Allow iteration over tasks in execution plan"""
        return iter(self.tasks)

    def log_status(self) -> None:
        """Log status of all tasks in execution plan, showing completion and dependency status"""
        check = "\u2714"  # ✔
        cross = "\u2718"  # ✘
        status = f"\n{'Job Name':<50} {'Partition Date':<15} {'Complete':<8} {'Dependencies':<12}\n"
        status = status + ("-" * 70 + "\n")
        for task in self.tasks:
            complete_icon = check if task.completion else cross
            deps_icon = check if task.dependencies_complete else cross
            status = status + f"{task.op:<50} {str(task.partition_date):<15} {complete_icon:^8} {deps_icon:^12}\n"
        logger.info(status)

    def check_completion(self, pipeline: Task, checker: DataChecker, rerun: bool) -> None:
        """
        Check if pipeline is complete by checking if target data exists for partition date

        :param pipeline: Pipeline object for which to check completion
        :param checker: DataChecker instance to use for checking data presence
        :param rerun: If True, skip completion check to allow re-running of completed pipelines

        :return: None, updates self.completion attribute
        """
        if not rerun:
            pipeline.completion = checker.check_date(pipeline.target, pipeline.partition_date)
            logger.info(
                f"Job {pipeline.op} completion status for partition date "
                f"{pipeline.partition_date}: {pipeline.completion}"
            )

    def check_pipeline_dependencies(self, pipeline: Task, checker: DataChecker, skip_dependencies: bool) -> None:
        """
        Check if dependencies are complete by checking if data exists for each dependency in date range

        :param pipeline: Pipeline object for which to check dependencies
        :param checker: DataChecker instance to use for checking data presence
        :param skip_dependencies: Skip dependency checks to allow running pipelines with incomplete dependencies

        :return: None, updates self.dependencies_complete attribute
        """
        if not skip_dependencies:
            for dependency in pipeline.dependencies:
                dependency.complete = checker.check_range(dependency.table, dependency.date_from, dependency.date_until)
                logger.info(
                    f"Dependency {dependency.table.get_table_path()} completion status for date range "
                    f"{dependency.date_from} - {dependency.date_until}: {dependency.complete}"
                )
        pipeline.dependencies_complete = all([dependency.complete for dependency in pipeline.dependencies])
