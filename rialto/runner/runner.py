#  Copyright 2022-2026 ABSA Group Limited
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

from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from rialto.runner.engine import RunnerEngine
from rialto.runner.runner_services import DefaultRunnerServices, RunnerServices


class Runner:
    """Entry point for pipeline execution orchestration (beginner-friendly API)"""

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
        services: RunnerServices = None,
    ):
        """
        Initialize Runner for pipeline orchestration.

        :param spark: SparkSession instance
        :param config_path: Path to pipeline configuration YAML
        :param run_date: Override run date (optional)
        :param rerun: Force re-execution of completed tasks
        :param op: Target specific pipeline by name (optional)
        :param skip_dependencies: Skip dependency validation
        :param overrides: Configuration overrides
        :param merge_schema: Enable schema merging in writer
        :param services: Custom RunnerServices bundle (optional, for advanced users)
        """
        self._services = services or DefaultRunnerServices.build(
            spark=spark,
            config_path=config_path,
            run_date=run_date,
            merge_schema=merge_schema,
            overrides=overrides,
        )
        self._engine = RunnerEngine(
            services=self._services,
            rerun=rerun,
            skip_dependencies=skip_dependencies,
        )
        self.op = op

    def __call__(self):
        """Execute pipelines"""
        self._engine.run(self.op)

    def dry_run(self):
        """Dry run - log status of pipelines without executing"""
        self._engine.dry_run_execution(self.op)

    def _debug(self) -> DataFrame:
        """Debug mode - run only first op for one date and return the resulting dataframe"""
        return self._engine.debug_first_task(self.op)
