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

__all__ = ["datasource", "job", "config_parser"]

import typing

import importlib_metadata
from loguru import logger

from rialto.common.utils import get_caller_module
from rialto.jobs.job_base import JobBase, JobMetadata
from rialto.jobs.module_register import ModuleRegister


def config_parser(cf_getter: typing.Callable) -> typing.Callable:
    """
    Config parser functions decorator.

    Registers a config parsing function into a rialto job prerequisite.
    You can then request the job via job function arguments.

    :param cf_getter:  dataset reader function
    :return: raw function, unchanged
    """
    ModuleRegister.register_callable(cf_getter)
    return cf_getter


def datasource(ds_getter: typing.Callable) -> typing.Callable:
    """
    Dataset reader functions decorator.

    Registers a data-reading function into a rialto job prerequisite.
    You can then request the job via job function arguments.

    :param ds_getter:  dataset reader function
    :return: raw reader function, unchanged
    """
    ModuleRegister.register_callable(ds_getter)
    return ds_getter


def _get_job_metadata(module: typing.Any) -> JobMetadata:
    try:
        package_name, _, _ = module.__name__.partition(".")
        dist_name = importlib_metadata.packages_distributions()[package_name][0]
        dist_version = importlib_metadata.version(dist_name)

        return JobMetadata(job_name=module.__name__, dist_name=dist_name, dist_version=dist_version)

    except Exception:
        logger.warning(f"Failed to get version of {module.__name__}! Will use N/A")
        return JobMetadata(job_name=module.__name__, dist_name="N/A", dist_version="N/A")


def _generate_rialto_job(
    callable: typing.Callable, module: object, class_name: str, metadata: JobMetadata, disable_version: bool
) -> typing.Type:
    generated_class = type(
        class_name,
        (JobBase,),
        {
            "get_custom_callable": lambda _: callable,
            "get_job_metadata": lambda _: metadata,
            "get_job_name": lambda _: class_name,
            "get_disable_version": lambda _: disable_version,
        },
    )

    generated_class.__module__ = module.__name__
    setattr(module, class_name, generated_class)

    logger.info(f"Registered {class_name} in {module}")
    return generated_class


def job(*args, custom_name=None, disable_version=False):
    """
    Rialto jobs decorator.

    Transforms a python function into a rialto transformation, which can be imported and ran by Rialto Runner.
    Is mainly used as @job and the function's name is used, and the outputs get automatic.
    To override this behavior, use @job(custom_name=XXX, disable_version=True).


    :param *args:  list of positional arguments. Empty in case custom_name or disable_version is specified.
    :param custom_name:  str for custom job name.
    :param disable_version:  bool for disabling automatically filling the VERSION column in the job's outputs.
    :return: One more job wrapper for run function (if custom name or version override specified).
             Otherwise, generates Rialto Transformation Type and returns it for in-module registration.
    """
    module = get_caller_module()
    metadata = _get_job_metadata(module)

    # Use case where it's just raw @f. Otherwise, we get [] here.
    if len(args) == 1 and callable(args[0]):
        f = args[0]
        return _generate_rialto_job(
            callable=f, module=module, class_name=f.__name__, metadata=metadata, disable_version=disable_version
        )

    # If custom args are specified, we need to return one more wrapper
    def inner_wrapper(f):
        # Setting default custom name, in case user only disables version
        class_name = f.__name__

        # User - Specified custom name
        if custom_name is not None:
            class_name = custom_name

        return _generate_rialto_job(
            callable=f, module=module, class_name=class_name, metadata=metadata, disable_version=disable_version
        )

    return inner_wrapper
