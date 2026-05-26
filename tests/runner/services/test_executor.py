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
from datetime import date
from unittest.mock import MagicMock, patch

from rialto.runner.services.executor import PipelineExecutor
from rialto.runner.services.task_registry import PipelineTask


def test_execute_calls_job_run_and_returns_df():
    mock_spark = MagicMock()
    mock_reader = MagicMock()
    mock_checker = MagicMock()
    executor = PipelineExecutor(mock_spark, mock_reader, mock_checker)

    config = MagicMock()
    config.module = "some.module"

    task = PipelineTask(
        op="test_op",
        execution_date=date(2026, 1, 1),
        partition_date=date(2026, 1, 1),
        config=config,
        target=MagicMock(),
    )

    mock_job = MagicMock()
    mock_df = MagicMock()
    mock_job.run.return_value = mock_df

    with patch.object(executor, "_load_module", return_value=mock_job) as load_module, patch.object(
        executor, "_init_tools", return_value=(None, None)
    ):
        result = executor.execute(task)

    load_module.assert_called_once_with("some.module")
    mock_job.run.assert_called_once()
    assert result == mock_df
