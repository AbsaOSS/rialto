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

__all__ = ["TaskResultMapper"]

from datetime import datetime

from rialto.runner.reporting.record import Record
from rialto.runner.task_registry import PipelineTask


class TaskResultMapper:
    """Maps task execution outcomes to Record objects with consistent schema"""

    @staticmethod
    def success(
        task: PipelineTask,
        run_start: datetime,
        records_count: int,
    ) -> Record:
        """Map successful task execution to Record"""
        return Record(
            job=task.op,
            target=task.target.get_table_path(),
            date=task.partition_date,
            time=datetime.now() - run_start,
            records=records_count,
            status="Success",
            reason="OK",
            exception=None,
        )

    @staticmethod
    def already_complete(task: PipelineTask, run_start: datetime) -> Record:
        """Map skipped (already complete) task to Record"""
        return Record(
            job=task.op,
            target=task.target.get_table_path(),
            date=task.partition_date,
            time=datetime.now() - run_start,
            records=0,
            status="Skipped",
            reason="AlreadyComplete",
            exception=None,
        )

    @staticmethod
    def dependencies_incomplete(
        task: PipelineTask,
        run_start: datetime,
        failed_deps: list,
    ) -> Record:
        """Map dependency failure to Record"""
        details = ",\n".join(failed_deps) if failed_deps else "Unknown"
        return Record(
            job=task.op,
            target=task.target.get_table_path(),
            date=task.partition_date,
            time=datetime.now() - run_start,
            records=0,
            status="Failed",
            reason="Dependencies Incomplete",
            exception=details,
        )

    @staticmethod
    def exception(
        task: PipelineTask,
        run_start: datetime,
        exception_message: str,
        traceback_str: str,
    ) -> Record:
        """Map exception during execution to Record"""
        return Record(
            job=task.op,
            target=task.target.get_table_path(),
            date=task.partition_date,
            time=datetime.now() - run_start,
            records=0,
            status="Error",
            reason=exception_message,
            exception=traceback_str,
        )

    @staticmethod
    def interrupted(task: PipelineTask, run_start: datetime) -> Record:
        """Map keyboard interrupt to Record"""
        return Record(
            job=task.op,
            target=task.target.get_table_path(),
            date=task.partition_date,
            time=datetime.now() - run_start,
            records=0,
            status="Error",
            reason="Keyboard Interrupt",
            exception=None,
        )
