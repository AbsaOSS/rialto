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

from rialto.runner.writer import Writer


@pytest.fixture
def sample_multi_partition(spark):
    df = spark.createDataFrame(
        [
            ("REGION_A", 3, date(2023, 1, 1), 100),
            ("REGION_A", 3, date(2023, 1, 1), 300),
        ],
        schema="region string, version int, info_date date, value int",
    )
    return df


@pytest.fixture
def sample_multi_partition_non_unique(spark):
    df = spark.createDataFrame(
        [
            ("REGION_A", 1, date(2023, 1, 1), 100),
            ("REGION_A", 2, date(2023, 1, 1), 300),
        ],
        schema="region string, version int, info_date date, value int",
    )
    return df


def test_replace_condition(sample_multi_partition):
    writer = Writer(spark=None)
    condition = writer._get_replace_condition(sample_multi_partition, partition_cols=["region", "version", "info_date"])
    expected_condition = "region = 'REGION_A' AND version = 3 AND info_date = '2023-01-01'"
    assert condition == expected_condition


def test_replace_condition_non_unique(sample_multi_partition_non_unique):
    writer = Writer(spark=None)
    with pytest.raises(ValueError):
        writer._get_replace_condition(
            sample_multi_partition_non_unique, partition_cols=["region", "version", "info_date"]
        )
