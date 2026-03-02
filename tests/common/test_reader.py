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

import pytest

from rialto.common.table_reader import TableReader


@pytest.fixture
def sample_df(spark):
    df = spark.createDataFrame(
        [(1, 2.33, "str", 4.55, 5.66), (1, 2.33, "str", 4.55, 5.66), (1, 2.33, "str", 4.55, 5.66)],
        schema="a long, b float, c string, d float, e float",
    )

    return df


@pytest.fixture
def multi_partition_df(spark):
    df = spark.createDataFrame(
        [
            ("REGION_A", "TYPE_X", date(2023, 1, 1), 100),
            ("REGION_A", "TYPE_X", date(2023, 1, 2), 200),
            ("REGION_A", "TYPE_Y", date(2023, 1, 1), 150),
            ("REGION_A", "TYPE_Y", date(2023, 1, 2), 250),
            ("REGION_B", "TYPE_X", date(2023, 1, 1), 300),
            ("REGION_B", "TYPE_X", date(2023, 1, 2), 400),
        ],
        schema="region string, type string, info_date date, value int",
    )
    return df


def test_uppercase_columns(spark, sample_df):
    tr = TableReader(spark)
    df = tr._uppercase_column_names(sample_df)
    assert df.columns == ["A", "B", "C", "D", "E"]


def test_get_latest_with_single_filter(multi_partition_df, mocker):
    mock_spark = mocker.MagicMock()
    mock_spark.read.table.return_value = multi_partition_df

    tr = TableReader(mock_spark)
    result = tr.get_latest("test_table", date_column="info_date", filters={"region": "REGION_A"})

    assert result.count() == 2
    assert all(row.info_date == date(2023, 1, 2) for row in result.collect())
    assert all(row.region == "REGION_A" for row in result.collect())


def test_get_latest_with_multiple_filters(multi_partition_df, mocker):
    mock_spark = mocker.MagicMock()
    mock_spark.read.table.return_value = multi_partition_df

    tr = TableReader(mock_spark)
    result = tr.get_latest("test_table", date_column="info_date", filters={"region": "REGION_A", "type": "TYPE_X"})

    assert result.count() == 1
    assert result.first().info_date == date(2023, 1, 2)
    assert result.first().region == "REGION_A"
    assert result.first().type == "TYPE_X"
    assert result.first().value == 200


def test_get_latest_without_filters(multi_partition_df, mocker):
    mock_spark = mocker.MagicMock()
    mock_spark.read.table.return_value = multi_partition_df

    tr = TableReader(mock_spark)
    result = tr.get_latest("test_table", date_column="info_date")

    assert result.count() == 3
    assert all(row.info_date == date(2023, 1, 2) for row in result.collect())
