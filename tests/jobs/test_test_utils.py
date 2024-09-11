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
import tests.jobs.resolver_dep_checks_job.cross_dep_tests_job_a as cross_dep_tests_job_a
import tests.jobs.resolver_dep_checks_job.cross_dep_tests_job_b as cross_dep_tests_job_b
import tests.jobs.resolver_dep_checks_job.dependency_tests_job as dependency_tests_job
import tests.jobs.test_job.test_job as test_job
from rialto.jobs.resolver import Resolver
from rialto.jobs.test_utils import disable_job_decorators, resolver_resolves


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


def test_resolver_dep_separation_correct_load_existing(spark):
    assert resolver_resolves(spark, cross_dep_tests_job_a.ok_dep_job)


def test_resolver_dep_separation_fail_load_missing(spark):
    with pytest.raises(Exception) as exc_info:
        assert resolver_resolves(spark, cross_dep_tests_job_b.missing_dep_job)
    assert exc_info is not None


def test_resolver_wont_cross_pollinate(spark):
    # This job has imported the dependencies
    assert resolver_resolves(spark, cross_dep_tests_job_a.ok_dep_job)

    # This job has no imported dependencies
    with pytest.raises(Exception) as exc_info:
        assert resolver_resolves(spark, cross_dep_tests_job_b.missing_dep_job)
    assert exc_info is not None
