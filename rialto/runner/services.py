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

__all__ = ["RunnerServices", "DefaultRunnerServices"]

from dataclasses import dataclass

from pyspark.sql import SparkSession

from rialto.common import TableReader
from rialto.runner.config_loader import ConfigLoader, PipelinesConfig
from rialto.runner.data_checker import DataChecker
from rialto.runner.date_manager import DateManager
from rialto.runner.executor import PipelineExecutor
from rialto.runner.reporting.tracker import Tracker
from rialto.runner.task_registry import TaskRegistry
from rialto.runner.task_status_checker import TaskStatusChecker
from rialto.runner.writer import DatabricksWriter


@dataclass
class RunnerServices:
    """Bundle of collaborator services for Runner orchestration"""

    config: PipelinesConfig
    date_manager: DateManager
    writer: DatabricksWriter
    data_checker: DataChecker
    task_checker: TaskStatusChecker
    registry: TaskRegistry
    executor: PipelineExecutor
    tracker: Tracker


class DefaultRunnerServices:
    """Factory for default Runner services composition"""

    @staticmethod
    def build(
        spark: SparkSession,
        config_path: str,
        run_date: str = None,
        merge_schema: bool = False,
        overrides: dict = None,
    ) -> RunnerServices:
        """
        Build default services for Runner.

        :param spark: SparkSession instance
        :param config_path: Path to pipeline configuration YAML
        :param run_date: Override run date (optional)
        :param merge_schema: Enable schema merging in writer
        :param overrides: Configuration overrides
        :return: RunnerServices bundle
        """
        config = ConfigLoader().load_yaml(config_path, overrides)
        date_manager = DateManager(config.runner, run_date)
        writer = DatabricksWriter(spark, merge_schema=merge_schema)

        reader = TableReader(spark)
        data_checker = DataChecker(reader)
        task_checker = TaskStatusChecker(data_checker)
        registry = TaskRegistry(spark, date_manager=date_manager)
        executor = PipelineExecutor(spark=spark, reader=reader, checker=data_checker)
        tracker = Tracker(
            mail_cfg=config.runner.mail,
            bookkeeping=config.runner.bookkeeping,
            spark=spark,
        )

        return RunnerServices(
            config=config,
            date_manager=date_manager,
            writer=writer,
            data_checker=data_checker,
            task_checker=task_checker,
            registry=registry,
            executor=executor,
            tracker=tracker,
        )
