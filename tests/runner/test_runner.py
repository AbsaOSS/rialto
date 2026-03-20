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
import pytest
from pyspark.sql import DataFrame

import rialto.runner.utils as utils
from rialto.common.table_reader import TableReader
from rialto.runner.runner import DateManager, Runner
from rialto.runner.table import Table
from tests.runner.runner_resources import (
    dep1_data,
    dep2_data,
    general_schema,
    multi_part_data,
    multi_schema,
    simple_group_data,
)
from tests.runner.transformations.simple_group import SimpleGroup


class MockReader(TableReader):
    def __init__(self, spark):
        self.spark = spark

    def _get_raw_data(self, table: str) -> DataFrame:
        if table == "catalog.schema.simple_group":
            return self.spark.createDataFrame(simple_group_data, general_schema)
        if table == "source.schema.dep1":
            return self.spark.createDataFrame(dep1_data, general_schema)
        if table == "source.schema.dep2":
            return self.spark.createDataFrame(dep2_data, general_schema)
        if table == "source.schema.multi_part_data":
            return self.spark.createDataFrame(multi_part_data, multi_schema)


def test_table_exists(spark, mocker):
    mock = mocker.patch("pyspark.sql.Catalog.tableExists", return_value=True)
    utils.table_exists(spark, "abc")
    mock.assert_called_once_with("abc")


def test_load_module(spark, basic_runner):
    module = utils.load_module(basic_runner.config.pipelines[0].module)
    assert isinstance(module, SimpleGroup)


def test_generate(spark, mocker, basic_runner):
    run = mocker.patch("tests.runner.transformations.simple_group.SimpleGroup.run")
    group = SimpleGroup()
    config = basic_runner.config.pipelines[0]
    basic_runner._execute(group, DateManager.str_to_date("2023-01-31"), config)

    run.assert_called_once_with(
        reader=basic_runner.reader,
        run_date=DateManager.str_to_date("2023-01-31"),
        spark=spark,
        config=config,
        metadata_manager=None,
        feature_loader=None,
    )


def test_generate_w_dep(spark, mocker, basic_runner):
    run = mocker.patch("tests.runner.transformations.simple_group.SimpleGroup.run")
    group = SimpleGroup()
    basic_runner._execute(group, DateManager.str_to_date("2023-01-31"), basic_runner.config.pipelines[2])
    run.assert_called_once_with(
        reader=basic_runner.reader,
        run_date=DateManager.str_to_date("2023-01-31"),
        spark=spark,
        config=basic_runner.config.pipelines[2],
        metadata_manager=None,
        feature_loader=None,
    )


def test_init_dates(spark):
    runner = Runner(spark, config_path="tests/runner/transformations/config.yaml", run_date="2023-03-31")
    assert runner.date_from == DateManager.str_to_date("2023-01-31")
    assert runner.date_until == DateManager.str_to_date("2023-03-31")

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
        overrides={"runner.watched_period_units": "weeks", "runner.watched_period_value": 2},
    )
    assert runner.date_from == DateManager.str_to_date("2023-03-17")
    assert runner.date_until == DateManager.str_to_date("2023-03-31")

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config2.yaml",
        run_date="2023-03-31",
    )
    assert runner.date_from == DateManager.str_to_date("2023-02-24")
    assert runner.date_until == DateManager.str_to_date("2023-03-31")


def test_completion(spark, mocker, basic_runner):
    mocker.patch("rialto.runner.utils.table_exists", return_value=True)

    basic_runner.reader = MockReader(spark)

    dates = ["2023-02-26", "2023-03-05", "2023-03-12", "2023-03-19", "2023-03-26"]
    dates = [DateManager.str_to_date(d) for d in dates]

    comp = basic_runner._get_completion(Table(table_path="catalog.schema.simple_group", partition="DATE"), dates)
    expected = [False, True, True, True, False]
    assert comp == expected


def test_completion_rerun(spark, mocker, basic_runner):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(spark, config_path="tests/runner/transformations/config.yaml", run_date="2023-03-31")
    runner.reader = MockReader(spark)

    dates = ["2023-02-26", "2023-03-05", "2023-03-12", "2023-03-19", "2023-03-26"]
    dates = [DateManager.str_to_date(d) for d in dates]

    comp = runner._get_completion(Table(table_path="catalog.schema.simple_group", partition="DATE"), dates)
    expected = [False, True, True, True, False]
    assert comp == expected


def test_completion_secondary_partitions(spark, mocker, basic_runner):
    mocker.patch("rialto.runner.utils.table_exists", return_value=True)

    basic_runner.reader = MockReader(spark)

    dates = ["2023-02-26", "2023-03-05", "2023-03-12", "2023-03-19", "2023-03-26"]
    dates = [DateManager.str_to_date(d) for d in dates]
    filters = {"version": 1, "type": "A"}

    comp = basic_runner._get_completion(
        Table(table_path="source.schema.multi_part_data", partition="DATE"), dates, filters
    )
    expected = [False, True, False, False, False]
    assert comp == expected


def test_completion_secondary_partitions_no_filter(spark, mocker, basic_runner):
    mocker.patch("rialto.runner.utils.table_exists", return_value=True)

    basic_runner.reader = MockReader(spark)

    dates = ["2023-02-26", "2023-03-05", "2023-03-12", "2023-03-19", "2023-03-26"]
    dates = [DateManager.str_to_date(d) for d in dates]

    comp = basic_runner._get_completion(
        Table(table_path="source.schema.multi_part_data", partition="DATE", secondary_partitions=["VERSION"]), dates
    )
    expected = [False, False, False, False, False]
    assert comp == expected


def test_check_dates_have_partition(spark, mocker):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
    )
    runner.reader = MockReader(spark)
    dates = ["2023-03-04", "2023-03-05", "2023-03-06"]
    dates = [DateManager.str_to_date(d) for d in dates]
    res = runner.check_dates_have_data(Table(schema_path="source.schema", table="dep1", partition="DATE"), dates)
    expected = [False, True, False]
    assert res == expected


def test_check_dates_have_partition_no_table(spark, mocker):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=False)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
    )
    dates = ["2023-03-04", "2023-03-05", "2023-03-06"]
    dates = [DateManager.str_to_date(d) for d in dates]
    res = runner.check_dates_have_data(Table(schema_path="source.schema", table="dep66", partition="DATE"), dates)
    expected = [False, False, False]
    assert res == expected


@pytest.mark.parametrize(
    "r_date, expected",
    [("2023-02-26", False), ("2023-03-05", True)],
)
def test_check_dependencies(spark, mocker, r_date, expected):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
    )
    runner.reader = MockReader(spark)
    res = runner.check_dependencies(runner.config.pipelines[0], DateManager.str_to_date(r_date))
    assert res == expected


@pytest.mark.parametrize(
    "r_date, expected",
    [("2023-03-19", True), ("2023-03-18", False)],
)
def test_check_dependencies_filter(spark, mocker, r_date, expected):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config3.yaml",
        run_date="2023-03-19",
    )
    runner.reader = MockReader(spark)
    res = runner.check_dependencies(runner.config.pipelines[0], DateManager.str_to_date(r_date))
    assert res == expected


def test_check_no_dependencies(spark, mocker):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
    )
    runner.reader = MockReader(spark)
    res = runner.check_dependencies(runner.config.pipelines[1], DateManager.str_to_date("2023-03-05"))
    assert res is True


def test_select_dates(spark, mocker):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
        overrides={"runner.watched_period_units": "months", "runner.watched_period_value": 1},
    )
    runner.reader = MockReader(spark)

    r, i = runner._select_run_dates(
        runner.config.pipelines[0], Table(table_path="catalog.schema.simple_group", partition="DATE")
    )
    expected_run = ["2023-03-05", "2023-03-12", "2023-03-19", "2023-03-26"]
    expected_run = [DateManager.str_to_date(d) for d in expected_run]
    expected_info = ["2023-03-02", "2023-03-09", "2023-03-16", "2023-03-23"]
    expected_info = [DateManager.str_to_date(d) for d in expected_info]
    assert r == expected_run
    assert i == expected_info


def test_select_dates_all_done(spark, mocker):
    mocker.patch("rialto.runner.runner.utils.table_exists", return_value=True)

    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-02",
        overrides={"runner.watched_period_units": "months", "runner.watched_period_value": 0},
    )
    runner.reader = MockReader(spark)

    r, i = runner._select_run_dates(
        runner.config.pipelines[0], Table(table_path="catalog.schema.simple_group", partition="DATE")
    )
    expected_run = []
    expected_run = [DateManager.str_to_date(d) for d in expected_run]
    expected_info = []
    expected_info = [DateManager.str_to_date(d) for d in expected_info]
    assert r == expected_run
    assert i == expected_info


def test_op_selected(spark, mocker):
    mocker.patch("rialto.runner.reporting.tracker.Tracker.report_by_mail")
    run = mocker.patch("rialto.runner.runner.Runner._run_pipeline")

    runner = Runner(spark, config_path="tests/runner/transformations/config.yaml", op="SimpleGroup")

    runner()
    run.assert_called_once()


def test_op_bad(spark, mocker):
    mocker.patch("rialto.runner.reporting.tracker.Tracker.report_by_mail")
    mocker.patch("rialto.runner.runner.Runner._run_pipeline")

    runner = Runner(spark, config_path="tests/runner/transformations/config.yaml", op="BadOp")

    with pytest.raises(ValueError) as exception:
        runner()
    assert str(exception.value) == "Unknown operation selected: BadOp"


def test_bookkeeping_active(spark, mocker):
    mocker.patch("rialto.runner.runner.Runner._run_pipeline")

    runner = Runner(spark, config_path="tests/runner/transformations/config.yaml")
    assert runner.config.runner.bookkeeping == "some.test.location"


def test_bookkeeping_inactive(spark, mocker):
    mocker.patch("rialto.runner.runner.Runner._run_pipeline")

    runner = Runner(spark, config_path="tests/runner/transformations/config2.yaml")
    assert runner.config.runner.bookkeeping is None


def test_config_multi_partition(spark, mocker):
    runner = Runner(spark, config_path="tests/runner/transformations/config3.yaml")
    assert runner.config.pipelines[0].target.secondary_partition_columns == ["VERSION", "ENV"]
    assert runner.config.pipelines[0].dependencies[0].filters == {"VERSION": 2, "TYPE": "A"}
    assert runner.config.pipelines[0].target.rerun_filters == {"version": 3, "env": "dev"}
