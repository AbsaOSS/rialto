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

import rialto.jobs.decorators as decorators
import tests.jobs.test_job.test_job as test_job
import tests.jobs.test_job.dependency_tests_job as dependency_tests_job
from rialto.jobs.decorators.resolver import Resolver
from rialto.jobs.decorators.test_utils import disable_job_decorators, resolver_resolves


def test_raw_dataset_patch(mocker):
    spy_rc = mocker.spy(Resolver, "register_callable")
    spy_dec = mocker.spy(decorators, "datasource")

    with disable_job_decorators(test_job):
        assert test_job.dataset() == "dataset_return"

        spy_dec.assert_not_called()
        spy_rc.assert_not_called()


def test_job_function_patch(mocker):
    spy_dec = mocker.spy(decorators, "job")

    with disable_job_decorators(test_job):
        assert test_job.job_function() == "job_function_return"

        spy_dec.assert_not_called()


def test_custom_name_job_function_patch(mocker):
    spy_dec = mocker.spy(decorators, "job")

    with disable_job_decorators(test_job):
        assert test_job.custom_name_job_function() == "custom_job_name_return"

        spy_dec.assert_not_called()


def test_resolver_resolves_ok_job(spark):
    assert resolver_resolves(spark, dependency_tests_job.ok_dependency_job)


def test_resolver_resolves_default_dependency(spark):
    assert resolver_resolves(spark, dependency_tests_job.default_dependency_job)


def test_resolver_resolves_fails_circular_dependency(spark):
    with pytest.raises(Exception) as exc_info:
        assert resolver_resolves(spark, dependency_tests_job.circular_dependency_job)

    assert exc_info is not None
    assert str(exc_info.value) == "Circular Dependence on circle_1!"


def test_resolver_resolves_fails_missing_dependency(spark):
    with pytest.raises(Exception) as exc_info:
        assert resolver_resolves(spark, dependency_tests_job.missing_dependency_job)

    assert exc_info is not None
    assert str(exc_info.value) == "x declaration not found!"
