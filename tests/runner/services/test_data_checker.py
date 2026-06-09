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
from datetime import date
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from rialto.common import TableReader
from rialto.runner.services.data_checker import DataChecker
from rialto.runner.services.table import Table


@pytest.fixture(scope="module")
def simple_dataframe(spark):
    df = [
        ("A", date(2023, 3, 5)),
        ("B", date(2023, 3, 12)),
        ("C", date(2023, 3, 19)),
    ]
    schema = StructType([StructField("KEY", StringType(), True), StructField("DATE", DateType(), True)])
    return spark.createDataFrame(df, schema=schema)


@pytest.fixture(scope="module")
def partitioned_dataframe(spark):
    df = [
        ("W", 1, "A", date(2023, 3, 5)),
        ("E", 1, "B", date(2023, 3, 5)),
        ("R", 2, "B", date(2023, 3, 5)),
        ("T", 1, "B", date(2023, 3, 12)),
        ("Y", 2, "A", date(2023, 3, 19)),
    ]
    schema = StructType(
        [
            StructField("VALUE", StringType(), True),
            StructField("VERSION", IntegerType(), True),
            StructField("TYPE", StringType(), True),
            StructField("DATE", DateType(), True),
        ]
    )
    return spark.createDataFrame(df, schema=schema)


@pytest.fixture(scope="module")
def new_insert_partitioned_dataframe(spark):
    df = [
        ("E", 1, "B", date(2023, 3, 5)),
        ("T", 1, "B", date(2023, 3, 5)),
    ]
    schema = StructType(
        [
            StructField("VALUE", StringType(), True),
            StructField("VERSION", IntegerType(), True),
            StructField("TYPE", StringType(), True),
            StructField("DATE", DateType(), True),
        ]
    )
    return spark.createDataFrame(df, schema=schema)


@pytest.mark.parametrize(
    "partition_date, expected",
    [
        (date(2023, 3, 12), True),
        (date(2023, 3, 10), False),
        (date(2023, 3, 19), True),
        (date(2023, 3, 26), False),
    ],
)
def test_check_date(mocker, spark, simple_dataframe, partition_date, expected):
    mocker.patch("rialto.common.table_reader.TableReader.table_exists", return_value=True)
    mocker.patch("rialto.common.table_reader.TableReader._get_raw_data", return_value=simple_dataframe)

    data_checker = DataChecker(TableReader(spark))
    table = Table(table_path="catalog.schema.simple_group", partition="DATE")
    result = data_checker.check_date(table, partition_date)
    assert result == expected


@pytest.mark.parametrize(
    "start_date, end_date, expected",
    [
        (date(2023, 3, 12), date(2023, 4, 12), True),
        (date(2023, 3, 10), date(2023, 3, 11), False),
        (date(2023, 3, 19), date(2023, 3, 19), True),
        (date(2023, 3, 26), date(2023, 3, 29), False),
    ],
)
def test_check_range(mocker, spark, simple_dataframe, start_date, end_date, expected):
    mocker.patch("rialto.common.table_reader.TableReader.table_exists", return_value=True)
    mocker.patch("rialto.common.table_reader.TableReader._get_raw_data", return_value=simple_dataframe)

    data_checker = DataChecker(TableReader(spark))
    table = Table(table_path="catalog.schema.simple_group", partition="DATE")
    result = data_checker.check_range(table, start_date, end_date)
    assert result == expected


def test_check_range_no_table(
    mocker,
    spark,
):
    mocker.patch("rialto.common.table_reader.TableReader.table_exists", return_value=False)

    data_checker = DataChecker(TableReader(spark))
    table = Table(table_path="catalog.schema.simple_group", partition="DATE")
    result = data_checker.check_date(table, date(2023, 3, 12))
    assert result is False


@pytest.mark.parametrize(
    "partition_date, expected",
    [
        (date(2023, 2, 26), False),
        (date(2023, 3, 5), True),
        (date(2023, 3, 12), False),
        (date(2023, 3, 19), False),
        (date(2023, 3, 26), False),
    ],
)
def test_check_date_secondary_partitions_and_filters(mocker, spark, partitioned_dataframe, partition_date, expected):
    mocker.patch("rialto.common.table_reader.TableReader.table_exists", return_value=True)
    mocker.patch("rialto.common.table_reader.TableReader._get_raw_data", return_value=partitioned_dataframe)

    data_checker = DataChecker(TableReader(spark))
    table = Table(
        table_path="catalog.schema.simple_group",
        partition="DATE",
        secondary_partitions=["VERSION", "TYPE"],
        filters={"version": 1, "type": "A"},
    )
    result = data_checker.check_date(table, partition_date)
    assert result == expected


@pytest.mark.parametrize(
    "partition_date, expected",
    [
        (date(2023, 2, 26), False),
        (date(2023, 3, 5), False),
        (date(2023, 3, 12), False),
        (date(2023, 3, 19), False),
        (date(2023, 3, 26), False),
    ],
)
def test_check_date_secondary_partitions_no_filters(mocker, spark, partitioned_dataframe, partition_date, expected):
    mocker.patch("rialto.common.table_reader.TableReader.table_exists", return_value=True)
    mocker.patch("rialto.common.table_reader.TableReader._get_raw_data", return_value=partitioned_dataframe)

    data_checker = DataChecker(TableReader(spark))
    table = Table(
        table_path="catalog.schema.simple_group",
        partition="DATE",
        secondary_partitions=["VERSION", "TYPE"],
        filters=None,
    )
    result = data_checker.check_date(table, partition_date)
    assert result == expected


def test_check_written_with_no_filters_or_secondary_partitions():
    mock_reader = MagicMock()
    mock_df = MagicMock()
    mock_reader.get_table.return_value = mock_df
    mock_df.count.return_value = 42

    checker = DataChecker(mock_reader)
    table = Table(table_path="dummy.table.path", partition="DATE")
    result = checker.check_written(table, date(2023, 3, 5), MagicMock())
    assert result == 42
    mock_reader.get_table.assert_called_once_with(
        "dummy.table.path",
        date_column="DATE",
        date_from=date(2023, 3, 5),
        date_to=date(2023, 3, 5),
        filters={},
    )


def test_check_written_with_filters():
    mock_reader = MagicMock()
    mock_df = MagicMock()
    mock_reader.get_table.return_value = mock_df
    mock_df.count.return_value = 42

    checker = DataChecker(mock_reader)
    table = Table(table_path="dummy.table.path", partition="DATE", filters={"foo": "bar"})
    result = checker.check_written(table, date(2023, 3, 5), MagicMock())
    assert result == 42
    mock_reader.get_table.assert_called_once_with(
        "dummy.table.path",
        date_column="DATE",
        date_from=date(2023, 3, 5),
        date_to=date(2023, 3, 5),
        filters={"foo": "bar"},
    )


def test_check_written_with_secondary_partitions(mocker, new_insert_partitioned_dataframe):
    # Setup
    mock_reader = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 7
    mock_reader.get_table.return_value = mock_df

    checker = DataChecker(mock_reader)
    table = Table(
        table_path="dummy.table.path", partition="DATE", filters=None, secondary_partitions=["VERSION", "TYPE"]
    )
    result = checker.check_written(table, date(2023, 3, 5), new_insert_partitioned_dataframe)
    assert result == 7
    mock_reader.get_table.assert_called_once_with(
        "dummy.table.path",
        date_column="DATE",
        date_from=date(2023, 3, 5),
        date_to=date(2023, 3, 5),
        filters={"VERSION": 1, "TYPE": "B"},
    )
