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


import datetime
from unittest.mock import MagicMock

import pyspark.sql.types

import tests.jobs.resources as resources
from rialto.jobs.decorators.resolver import Resolver
from rialto.loader import DatabricksLoader, PysparkFeatureLoader


def test_setup_except_feature_loader(spark):
    table_reader = MagicMock()
    config = MagicMock()
    date = datetime.date(2023, 1, 1)

    resources.CustomJobNoReturnVal().run(reader=table_reader, run_date=date, spark=spark, config=config)

    assert Resolver.resolve("run_date") == date
    assert Resolver.resolve("config") == config
    assert Resolver.resolve("spark") == spark
    assert Resolver.resolve("table_reader") == table_reader


def test_setup_feature_loader(spark):
    table_reader = MagicMock()
    date = datetime.date(2023, 1, 1)
    feature_loader = PysparkFeatureLoader(spark, DatabricksLoader(spark, "", ""), "")

    resources.CustomJobNoReturnVal().run(
        reader=table_reader, run_date=date, spark=spark, config=None, feature_loader=feature_loader
    )

    assert type(Resolver.resolve("feature_loader")) == PysparkFeatureLoader


def test_custom_callable_called(spark, mocker):
    spy_cc = mocker.spy(resources, "custom_callable")

    table_reader = MagicMock()
    date = datetime.date(2023, 1, 1)

    resources.CustomJobNoReturnVal().run(reader=table_reader, run_date=date, spark=spark, config=None)

    spy_cc.assert_called_once()


def test_no_return_vaue_adds_version_timestamp_dataframe(spark):
    table_reader = MagicMock()
    date = datetime.date(2023, 1, 1)

    result = resources.CustomJobNoReturnVal().run(reader=table_reader, run_date=date, spark=spark, config=None)

    assert type(result) is pyspark.sql.DataFrame
    assert result.columns == ["JOB_NAME", "CREATION_TIME", "VERSION"]
    assert result.first()["VERSION"] == "job_version"
    assert result.count() == 1


def test_return_dataframe_forwarded_with_version(spark):
    table_reader = MagicMock()
    date = datetime.date(2023, 1, 1)

    result = resources.CustomJobReturnsDataFrame().run(reader=table_reader, run_date=date, spark=spark, config=None)

    assert type(result) is pyspark.sql.DataFrame
    assert result.columns == ["FIRST", "SECOND", "VERSION"]
    assert result.first()["VERSION"] == "job_version"
    assert result.count() == 2


def test_none_job_version_wont_fill_job_colun(spark):
    table_reader = MagicMock()
    date = datetime.date(2023, 1, 1)

    result = resources.CustomJobNoVersion().run(reader=table_reader, run_date=date, spark=spark, config=None)

    assert type(result) is pyspark.sql.DataFrame
    assert "VERSION" not in result.columns
