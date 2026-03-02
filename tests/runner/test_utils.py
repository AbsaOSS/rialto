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

import rialto.runner.utils as utils
from rialto.runner.table import Table


def test_get_table_version_nonexistent(mocker):
    mock_spark = mocker.MagicMock()
    mocker.patch("rialto.runner.utils.table_exists", return_value=False)

    version = utils.get_table_version(mock_spark, "catalog.schema.nonexistent")

    assert version is None


def test_get_table_version_exists(spark, mocker):
    mocker.patch("rialto.runner.utils.table_exists", return_value=True)

    mock_history_df = spark.createDataFrame(
        [(5, "2023-01-01", "user", "WRITE")],
        schema="version int, timestamp string, userId string, operation string",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_history_df

    version = utils.get_table_version(mock_spark, "catalog.schema.existing_table")

    assert version == 5
    mock_spark.sql.assert_called_once_with("DESCRIBE HISTORY catalog.schema.existing_table")


def test_get_available_dates_without_filters(spark, mocker):
    mock_partition_df = spark.createDataFrame(
        [
            ("2023-01-01",),
            ("2023-01-02",),
            ("2023-01-03",),
        ],
        schema="info_date string",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_partition_df

    table = Table(catalog="cat", schema="sch", table="tab", partitions="info_date")
    dates = utils.get_available_dates(mock_spark, table)

    assert len(dates) == 3
    assert dates == ["2023-01-01", "2023-01-03", "2023-01-02"]


def test_get_available_dates_with_single_filter(spark, mocker):
    mock_partition_df = spark.createDataFrame(
        [
            ("REGION_A", "2023-01-01"),
            ("REGION_A", "2023-01-02"),
            ("REGION_B", "2023-01-01"),
            ("REGION_B", "2023-01-02"),
        ],
        schema="region string, info_date string",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_partition_df

    table = Table(catalog="cat", schema="sch", table="tab", partitions=["region", "info_date"], date_column="info_date")
    dates = utils.get_available_dates(mock_spark, table, filters={"region": "REGION_A"})

    assert len(dates) == 2
    assert dates == ["2023-01-01", "2023-01-02"]


def test_get_available_dates_with_multiple_filters(spark, mocker):
    mock_partition_df = spark.createDataFrame(
        [
            ("REGION_A", "TYPE_X", "2023-01-01"),
            ("REGION_A", "TYPE_X", "2023-01-02"),
            ("REGION_A", "TYPE_Y", "2023-01-01"),
            ("REGION_B", "TYPE_X", "2023-01-01"),
        ],
        schema="region string, type string, info_date string",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_partition_df

    table = Table(
        catalog="cat", schema="sch", table="tab", partitions=["region", "type", "info_date"], date_column="info_date"
    )
    dates = utils.get_available_dates(mock_spark, table, filters={"region": "REGION_A", "type": "TYPE_X"})

    assert len(dates) == 2
    assert dates == ["2023-01-01", "2023-01-02"]


def test_get_available_dates_missing_date_column(spark, mocker):
    mock_partition_df = spark.createDataFrame(
        [
            ("2023-01-01",),
        ],
        schema="info_date string",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_partition_df

    table = Table(catalog="cat", schema="sch", table="tab", partitions="info_date", date_column="wrong_column")

    with pytest.raises(ValueError, match="date_column 'wrong_column' not found"):
        utils.get_available_dates(mock_spark, table)


def test_get_available_dates_missing_filter_column(spark, mocker):
    mock_partition_df = spark.createDataFrame(
        [
            ("REGION_A", "2023-01-01"),
        ],
        schema="region string, info_date string",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_partition_df

    table = Table(catalog="cat", schema="sch", table="tab", partitions=["region", "info_date"])

    with pytest.raises(ValueError, match="Filter column 'wrong_filter' not found"):
        utils.get_available_dates(mock_spark, table, filters={"wrong_filter": "value"})


def test_get_rows_from_history_with_output_rows(spark, mocker):
    from pyspark.sql import Row

    mock_history_df = spark.createDataFrame(
        [
            Row(version=5, operationMetrics={"numOutputRows": "1234"}),
            Row(version=4, operationMetrics={"numOutputRows": "500"}),
        ]
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_history_df

    rows = utils.get_rows_from_history(mock_spark, "catalog.schema.table", "5")

    assert rows == 1234
    mock_spark.sql.assert_called_once_with("DESCRIBE HISTORY catalog.schema.table")


def test_get_rows_from_history_no_output_rows(spark, mocker):
    from pyspark.sql import Row

    mock_history_df = spark.createDataFrame(
        [Row(version=5, operationMetrics={"numFiles": "10"})],
        schema="version int, operationMetrics map<string,string>",
    )

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_history_df

    rows = utils.get_rows_from_history(mock_spark, "catalog.schema.table", "5")

    assert rows == 0


def test_get_rows_from_history_version_not_found(spark, mocker):
    from pyspark.sql import Row

    mock_history_df = spark.createDataFrame([Row(version=4, operationMetrics={"numOutputRows": "500"})])

    mock_spark = mocker.MagicMock()
    mock_spark.sql.return_value = mock_history_df

    rows = utils.get_rows_from_history(mock_spark, "catalog.schema.table", "5")

    assert rows == 0
