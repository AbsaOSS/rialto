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

import importlib
from unittest.mock import patch

import pyspark.sql.functions as F

import tests.jobs.test_job.test_job as tj
from rialto.jobs.decorators import _get_job_metadata
from rialto.jobs.job_base import JobBase, JobMetadata
from rialto.jobs.module_register import ModuleRegister


def test_dataset_decorator():
    _ = importlib.import_module("tests.jobs.test_job.test_job")
    assert ModuleRegister.find_callable("dataset", "tests.jobs.test_job.test_job") is not None


def test_config_decorator():
    _ = importlib.import_module("tests.jobs.test_job.test_job")
    assert ModuleRegister.find_callable("custom_config", "tests.jobs.test_job.test_job") is not None


def _rialto_import_stub(module_name, class_name):
    module = importlib.import_module(module_name)
    class_obj = getattr(module, class_name)
    return class_obj()


def test_job_function_type():
    result_class = _rialto_import_stub("tests.jobs.test_job.test_job", "job_function")
    assert issubclass(type(result_class), JobBase)


def test_job_function_callables_filled():
    result_class = _rialto_import_stub("tests.jobs.test_job.test_job", "job_function")

    custom_callable = result_class.get_custom_callable()
    assert custom_callable() == "job_function_return"

    metadata = result_class.get_job_metadata()
    assert metadata.dist_version == "N/A"

    job_name = result_class.get_job_name()
    assert job_name == "job_function"


def test_get_metadata():
    lib_metadata = _get_job_metadata(F)
    assert lib_metadata.dist_name == "pyspark"
    assert lib_metadata.dist_version is not None
    assert lib_metadata.dist_version != "N/A"


def test_job_metadata_filled():
    with patch("rialto.jobs.decorators._get_job_metadata") as mock:
        mock.return_value = JobMetadata(job_name="job_name", dist_name="job_function", dist_version="1.0.0")

        ModuleRegister.remove_module(tj)
        importlib.reload(tj)

        result_class = _rialto_import_stub("tests.jobs.test_job.test_job", "job_function")

    metadata = result_class.get_job_metadata()
    assert metadata.dist_version == "1.0.0"

    ModuleRegister.remove_module(tj)
    importlib.reload(tj)


def test_custom_name_function():
    result_class = _rialto_import_stub("tests.jobs.test_job.test_job", "custom_job_name")
    assert issubclass(type(result_class), JobBase)

    custom_callable = result_class.get_custom_callable()
    assert custom_callable() == "custom_job_name_return"

    job_name = result_class.get_job_name()
    assert job_name == "custom_job_name"


def test_job_disabling_version():
    result_class = _rialto_import_stub("tests.jobs.test_job.test_job", "disable_version_job_function")
    assert issubclass(type(result_class), JobBase)

    job_version = result_class.get_disable_version()
    assert job_version == True


def test_job_dependencies_registered(spark):
    job_class = _rialto_import_stub("tests.jobs.test_job.test_job", "job_asking_for_all_deps")
    # asserts part of the run
    job_class.run(spark=spark, run_date=456, reader=789, config=123, metadata_manager=654, feature_loader=321)
