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
__all__ = ["TaskStatusChecker"]

from loguru import logger

from rialto.runner.data_checker import DataChecker
from rialto.runner.task_registry import PipelineTask


class TaskStatusChecker:
    """Handles completion and dependency checks for pipeline tasks."""

    def __init__(self, checker: DataChecker):
        self.checker = checker

    def check_completion(self, pipeline: PipelineTask) -> None:
        """
        Check if pipeline is complete by checking if target data exists for partition date

        :param pipeline: Pipeline object for which to check completion

        :return: None, updates self.completion attribute
        """
        pipeline.completion = self.checker.check_date(pipeline.target, pipeline.partition_date)
        logger.info(
            f"Job {pipeline.op} completion status for partition date "
            f"{pipeline.partition_date}: {pipeline.completion}"
        )

    def check_pipeline_dependencies(self, pipeline: PipelineTask) -> None:
        """
        Check if dependencies are complete by checking if data exists for each dependency in date range

        :param pipeline: Pipeline object for which to check dependencies

        :return: None, updates self.dependencies_complete attribute
        """
        for dependency in pipeline.dependencies:
            dependency.complete = self.checker.check_range(
                dependency.table, dependency.date_from, dependency.date_until
            )
            logger.info(
                f"Dependency {dependency.table.get_table_path()} completion status for date range "
                f"{dependency.date_from} - {dependency.date_until}: {dependency.complete}"
            )
        pipeline.dependencies_complete = all([dependency.complete for dependency in pipeline.dependencies])
