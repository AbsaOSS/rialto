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

__all__ = ["RunnerEngine"]

import traceback
from datetime import datetime
from typing import List

from loguru import logger
from pyspark.sql import DataFrame

from rialto.runner.runner_services import RunnerServices
from rialto.runner.services.config_loader import PipelineConfig
from rialto.runner.services.result_mapper import TaskResultMapper
from rialto.runner.services.task_registry import PipelineTask


class RunnerEngine:
    """Orchestrates pipeline execution lifecycle and task tracking"""

    def __init__(self, services: RunnerServices, rerun: bool, skip_dependencies: bool):
        self.services = services
        self.rerun = rerun
        self.skip_dependencies = skip_dependencies

    def select_pipelines(self, op: str = None) -> List[PipelineConfig]:
        """Select pipelines to run based on operation name"""
        if op:
            selected = [p for p in self.services.config.pipelines if p.name == op]
            if not selected:
                raise ValueError(f"Unknown operation selected: {op}")
            return selected
        return self.services.config.pipelines

    def register_tasks(self, pipelines: List[PipelineConfig]) -> None:
        """Register tasks for all pipelines and date combinations"""
        for pipeline in pipelines:
            for exec_date, partition_date in self.services.date_manager.get_execution_and_partition_dates(
                pipeline.schedule
            ):
                self.services.registry.add_task(
                    name=pipeline.name,
                    execution_date=exec_date,
                    partition_date=partition_date,
                    config=pipeline,
                )

    def check_tasks(self) -> None:
        """Check task completion and dependency status"""
        for task in self.services.registry.tasks:
            if not self.rerun:
                try:
                    self.services.task_checker.check_completion(task)
                except Exception as e:
                    logger.error(f"{task.name} completion check failed for {task.partition_date}:\n\t{e}")
                    task.precheck_failed = True
                    task.error = str(e)
                    task.error_trace = traceback.format_exc()
            if not self.skip_dependencies:
                try:
                    self.services.task_checker.check_pipeline_dependencies(task)
                except Exception as e:
                    logger.error(f"{task.name} dependency check failed for {task.partition_date}:\n\t{e}")
                    task.precheck_failed = True
                    task.error = str(e)
                    task.error_trace = traceback.format_exc()

    def log_task_status(self) -> None:
        """Log summary of task statuses"""
        self.services.registry.log_status()

    def run_tasks(self) -> None:
        """Execute runnable tasks with per-task error isolation"""
        for task in self.services.registry.tasks:
            logger.info(f"Executing task {task.name} for partition date {task.partition_date}")
            self._execute_task_with_tracking(task)

    def _execute_task_with_tracking(self, task: PipelineTask) -> None:
        """Execute single task with record tracking"""
        run_start = datetime.now()

        if task.precheck_failed:
            self.services.tracker.add(TaskResultMapper.exception(task, run_start, task.error, task.error_trace))
            return

        # Skip already-complete tasks
        if task.completion and not self.rerun:
            logger.info(f"Skipping task {task.name} for partition {task.partition_date} - already complete")
            self.services.tracker.add(TaskResultMapper.already_complete(task, run_start))
            return

        # Skip if dependencies not met
        incomplete_deps = [
            f"{dep.table.get_table_path()} from {dep.date_from} until {dep.date_until}"
            for dep in task.dependencies
            if not dep.complete
        ]
        if incomplete_deps and not self.skip_dependencies:
            logger.info(
                f"Incomplete dependencies for task {task.name} for "
                f"partition {task.partition_date} - {', '.join(incomplete_deps)}"
            )
            self.services.tracker.add(TaskResultMapper.dependencies_incomplete(task, run_start, incomplete_deps))
            return

        # Execute task
        try:
            df = self.services.executor.execute(task)
            self.services.writer.write(df, task.partition_date, task.target)
            records = self.services.data_checker.check_written(task.target, task.partition_date, df)
            logger.info(
                f"Task {task.name} for partition {task.partition_date} completed successfully with {records} records"
            )
            self.services.tracker.add(TaskResultMapper.success(task, run_start, records))
        except KeyboardInterrupt:
            self.services.tracker.add(TaskResultMapper.interrupted(task, run_start))
            raise
        except Exception as e:
            logger.exception(f"Task {task.name} failed for partition {task.partition_date}")
            self.services.tracker.add(TaskResultMapper.exception(task, run_start, str(e), traceback.format_exc()))

    def finalize(self) -> None:
        """Send final reports via mail/bookkeeping"""
        self.services.tracker.report_by_mail()
        self.log_task_status()

    def run(self, op: str = None) -> None:
        """Execute all tasks"""
        pipelines = self.select_pipelines(op)
        self.register_tasks(pipelines)
        self.check_tasks()
        self.log_task_status()
        self.run_tasks()
        self.finalize()

    def dry_run_execution(self, op: str = None) -> None:
        """Execute pre-run checks without task execution"""
        pipelines = self.select_pipelines(op)
        self.register_tasks(pipelines)
        self.check_tasks()
        self.log_task_status()

    def debug_first_task(self, op: str = None) -> DataFrame:
        """Debug mode: execute first task and return result"""
        pipelines = self.select_pipelines(op)
        self.register_tasks(pipelines)
        return self.services.executor.execute(self.services.registry.tasks[0])
