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
import typing

import pandas as pd

from rialto.jobs.job_base import JobBase
from rialto.jobs.resolver import Resolver


def custom_callable():
    return None


def asserting_callable():
    assert Resolver.resolve("run_date")
    assert Resolver.resolve("config")
    assert Resolver.resolve("spark")
    assert Resolver.resolve("table_reader")


class CustomJobNoReturnVal(JobBase):
    def get_job_name(self) -> str:
        return "job_name"

    def get_job_version(self) -> str:
        return "job_version"

    def get_custom_callable(self) -> typing.Callable:
        return custom_callable


class CustomJobReturnsDataFrame(CustomJobNoReturnVal):
    def get_custom_callable(self) -> typing.Callable:
        def f(spark):
            df = pd.DataFrame([["A", 1], ["B", 2]], columns=["FIRST", "SECOND"])

            return spark.createDataFrame(df)

        return f


class CustomJobNoVersion(CustomJobNoReturnVal):
    def get_job_version(self) -> str:
        return None


def CustomJobAssertResolverSetup(CustomJobNoReturnVal):
    def get_custom_callable():
        return asserting_callable
