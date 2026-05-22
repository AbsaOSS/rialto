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

from datetime import date, datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import Row

from rialto.runner.table import Table
from rialto.runner.writer import DatabricksWriter


@pytest.fixture
def writer(spark):
    return DatabricksWriter(spark, merge_schema=False)


@pytest.fixture
def writer_merge(spark):
    return DatabricksWriter(spark, merge_schema=True)


@pytest.fixture
def simple_table():
    return Table(schema_path="default.test_schema", class_name="MyTable", partition="info_date")


@pytest.fixture
def table_with_secondary(simple_table):
    simple_table.secondary_partitions = ["region"]
    return simple_table


# --- _align_schema ---


def test_align_schema_no_existing_columns_returns_df_unchanged(spark, writer):
    df = spark.createDataFrame([Row(a=1, b=2)])
    result = writer._align_schema(df, None)
    assert result.columns == df.columns


def test_align_schema_reorders_to_existing_columns(spark, writer):
    df = spark.createDataFrame([Row(a=1, b=2, c=3)])
    result = writer._align_schema(df, ["c", "a", "b"])
    assert result.columns == ["c", "a", "b"]


def test_align_schema_new_columns_appended_after_existing(spark, writer):
    df = spark.createDataFrame([Row(a=1, b=2, new_col=3)])
    result = writer._align_schema(df, ["a", "b"])
    assert result.columns == ["a", "b", "new_col"]


def test_align_schema_missing_existing_columns_raises_value_error(spark, writer):
    # column "gone" is in existing but not in df — should raise ValueError
    df = spark.createDataFrame([Row(a=1, b=2)])
    with pytest.raises(ValueError):
        writer._align_schema(df, ["gone", "a", "b"])


# --- _get_replace_condition ---


def test_get_replace_condition_string_value(spark, writer):
    df = spark.createDataFrame([Row(info_date=date(2020, 1, 1))])
    target = Table(schema_path="default.test_schema", class_name="MyTable", partition="info_date")
    condition = writer._get_replace_condition(df, target, datetime(2020, 1, 1))
    assert condition == "info_date = '2020-01-01'"


def test_get_replace_condition_second_value_no_filters(spark, writer):
    df = spark.createDataFrame([Row(information_date="2020-01-01", region=1)])
    target = Table(
        schema_path="default.test_schema",
        class_name="MyTable",
        partition="information_date",
        secondary_partitions=["region"],
    )
    condition = writer._get_replace_condition(df, target, datetime(2020, 1, 1))
    assert condition == "information_date = '2020-01-01' AND region = 1"


def test_get_replace_condition_second_value_with_filters(spark, writer):
    df = spark.createDataFrame([Row(information_date="2020-01-01", region=1)])
    target = Table(
        schema_path="default.test_schema",
        class_name="MyTable",
        partition="information_date",
        secondary_partitions=["region"],
        filters={"region": 1},
    )
    condition = writer._get_replace_condition(df, target, datetime(2020, 1, 1))
    assert condition == "information_date = '2020-01-01' AND region = 1"


def test_get_replace_condition_raises_on_multiple_distinct_values(spark, writer):
    df = spark.createDataFrame(
        [Row(information_date="2020-01-01", region=1), Row(information_date="2020-01-01", region=2)]
    )
    target = Table(
        schema_path="default.test_schema",
        class_name="MyTable",
        partition="information_date",
        secondary_partitions=["region"],
    )
    with pytest.raises(ValueError, match="more than 1 distinct value"):
        writer._get_replace_condition(df, target, datetime(2020, 1, 1))


# --- _process ---


def test_process_adds_partition_column(spark, writer, simple_table):
    df = spark.createDataFrame([Row(a=1)])
    with patch.object(writer, "_get_existing_columns", return_value=None):
        result = writer._process(df, date(2020, 1, 1), simple_table)
    assert "info_date" in result.columns
    assert result.collect()[0]["info_date"] == date(2020, 1, 1)


# --- write (integration of internal steps) ---


def test_write_calls_create_schema(spark, writer, simple_table):
    with patch.object(writer, "_create_schema") as mock_create, patch.object(writer, "_process"), patch.object(
        writer, "_get_replace_condition"
    ):
        df = Mock()
        df.write = Mock()
        writer.write(df, Mock(), simple_table)

    mock_create.assert_called_once_with(simple_table)


def test_write_merge_schema_option(spark, writer_merge, simple_table):
    df = MagicMock()
    df.write = MagicMock()

    with patch.object(writer_merge, "_create_schema"), patch.object(
        writer_merge, "_process", return_value=df
    ), patch.object(writer_merge, "_get_replace_condition"):
        writer_merge.write(df, date(2020, 1, 1), simple_table)

    option_calls = df.write.format.return_value.partitionBy.return_value.mode.return_value.option.call_args_list
    assert any(call.args == ("mergeSchema", "true") for call in option_calls)


def test_write_not_merge_schema_option(spark, writer, simple_table):
    df = MagicMock()
    df.write = MagicMock()

    with patch.object(writer, "_create_schema"), patch.object(writer, "_process", return_value=df), patch.object(
        writer, "_get_replace_condition"
    ):
        writer.write(df, date(2020, 1, 1), simple_table)

    option_calls = df.write.format.return_value.partitionBy.return_value.mode.return_value.option.call_args_list
    assert any(call.args == ("mergeSchema", "false") for call in option_calls)
