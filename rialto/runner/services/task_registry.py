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
__all__ = ["TaskRegistry", "PipelineTask", "PipelineDependency"]

from dataclasses import dataclass, field
from datetime import date
from typing import Iterator, List

from loguru import logger
from pyspark.sql import SparkSession

from rialto.runner.services.config_loader import PipelineConfig
from rialto.runner.services.date_manager import DateManager
from rialto.runner.services.table import Table


@dataclass
class PipelineDependency:
    """Class representing a pipeline dependency, with associated table and date range for checking completion"""

    table: Table
    date_from: date
    date_until: date
    complete: bool = False


@dataclass
class PipelineTask:
    """Class representing a pipeline to be executed."""

    op: str
    execution_date: date
    partition_date: date
    config: PipelineConfig
    target: Table
    dependencies: List[PipelineDependency] = field(default_factory=list)
    completion: bool = False
    dependencies_complete: bool = False


class TaskRegistry:
    """Registry for pipeline tasks to be executed"""

    def __init__(self, spark: SparkSession, date_manager: DateManager):
        self.spark = spark
        self.date_manager = date_manager
        self.tasks = []

    def add_task(self, name: str, execution_date: date, partition_date: date, config: PipelineConfig) -> None:
        """
        Add task to registry

        :param name: Name of the pipeline
        :param execution_date: Date when the pipeline is scheduled to run
        :param partition_date: Date for which the pipeline is processing data
        :param config: PipelineConfig object with pipeline configuration

        :return: None, adds a Pipeline object to self.tasks
        """
        target = Table.from_target_config(config)
        new_pipe = PipelineTask(
            op=name, execution_date=execution_date, partition_date=partition_date, config=config, target=target
        )

        for dependency_config in config.dependencies:
            dependency_table = Table.from_dependency_config(dependency_config)
            dependency_from = self.date_manager.date_subtract(
                execution_date, dependency_config.interval.units, dependency_config.interval.value
            )
            dependency = PipelineDependency(
                table=dependency_table, date_from=dependency_from, date_until=execution_date
            )
            new_pipe.dependencies.append(dependency)

        self.tasks.append(new_pipe)

    def __iter__(self) -> Iterator[PipelineTask]:
        """Allow iteration over tasks in execution plan"""
        return iter(self.tasks)

    def log_status(self) -> None:
        """Log status of all tasks in registry, showing completion and dependency status"""
        check = "\u2714"  # ✔
        cross = "\u2718"  # ✘
        status = f"\n{'Job Name':<50} {'Partition Date':<15} {'Complete':<8} {'Dependencies':<12}\n"
        status = status + ("-" * 70 + "\n")
        for task in self.tasks:
            complete_icon = check if task.completion else cross
            deps_icon = check if task.dependencies_complete else cross
            status = status + f"{task.op:<50} {str(task.partition_date):<15} {complete_icon:^8} {deps_icon:^12}\n"
        logger.info(status)
