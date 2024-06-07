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
from pyspark.sql import SparkSession

from rialto.runner import Runner


@pytest.fixture(scope="session")
def spark(request):
    """fixture for creating a spark session
    :param request: pytest.FixtureRequest object
    """

    spark = (
        SparkSession.builder.master("local[3]")
        .appName("pytest-pyspark-local-testing")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    request.addfinalizer(lambda: spark.stop())

    return spark


@pytest.fixture(scope="function")
def basic_runner(spark):
    return Runner(
        spark, config_path="tests/runner/transformations/config.yaml", feature_metadata_schema="", run_date="2023-03-31"
    )
