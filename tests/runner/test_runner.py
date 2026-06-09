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

from unittest.mock import MagicMock, patch

from rialto.runner.runner import Runner


def test_runner_init_builds_default_services_when_not_provided():
    spark = MagicMock()
    built_services = MagicMock()

    with (
        patch("rialto.runner.runner.DefaultRunnerServices.build", return_value=built_services) as build_mock,
        patch("rialto.runner.runner.RunnerEngine") as engine_cls,
    ):
        runner = Runner(
            spark=spark,
            config_path="tests/runner/transformations/config.yaml",
            run_date="2026-01-01",
            rerun=True,
            op="SimpleGroup",
            skip_dependencies=True,
            overrides={"runner": {"watched_period_units": "days", "watched_period_value": 7}},
            merge_schema=True,
        )

    build_mock.assert_called_once_with(
        spark=spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2026-01-01",
        merge_schema=True,
        overrides={"runner": {"watched_period_units": "days", "watched_period_value": 7}},
    )
    engine_cls.assert_called_once_with(
        services=built_services,
        rerun=True,
        skip_dependencies=True,
    )
    assert runner.op == "SimpleGroup"


def test_runner_init_uses_injected_services_without_build():
    spark = MagicMock()
    injected_services = MagicMock()

    with patch("rialto.runner.runner.DefaultRunnerServices.build") as build_mock, patch(
        "rialto.runner.runner.RunnerEngine"
    ) as engine_cls:
        Runner(
            spark=spark,
            config_path="tests/runner/transformations/config.yaml",
            services=injected_services,
        )

    build_mock.assert_not_called()
    engine_cls.assert_called_once_with(
        services=injected_services,
        rerun=False,
        skip_dependencies=False,
    )


def test_runner_call_delegates_to_engine_run():
    spark = MagicMock()
    injected_services = MagicMock()
    engine = MagicMock()

    with patch("rialto.runner.runner.RunnerEngine", return_value=engine):
        runner = Runner(
            spark=spark,
            config_path="tests/runner/transformations/config.yaml",
            op="SimpleGroup",
            services=injected_services,
        )

    runner()
    engine.run.assert_called_once_with("SimpleGroup")


def test_runner_dry_run_delegates_to_engine_dry_run_execution():
    spark = MagicMock()
    injected_services = MagicMock()
    engine = MagicMock()

    with patch("rialto.runner.runner.RunnerEngine", return_value=engine):
        runner = Runner(
            spark=spark,
            config_path="tests/runner/transformations/config.yaml",
            op="SimpleGroup",
            services=injected_services,
        )

    runner.dry_run()
    engine.dry_run_execution.assert_called_once_with("SimpleGroup")


def test_runner_debug_delegates_to_engine_and_returns_dataframe():
    spark = MagicMock()
    injected_services = MagicMock()
    engine = MagicMock()
    debug_df = MagicMock()
    engine.debug_first_task.return_value = debug_df

    with patch("rialto.runner.runner.RunnerEngine", return_value=engine):
        runner = Runner(
            spark=spark,
            config_path="tests/runner/transformations/config.yaml",
            op="SimpleGroup",
            services=injected_services,
        )

    result = runner._debug()

    engine.debug_first_task.assert_called_once_with("SimpleGroup")
    assert result is debug_df
