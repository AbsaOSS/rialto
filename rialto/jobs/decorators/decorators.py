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

__all__ = ["datasource", "job"]

import importlib.metadata
import inspect
import typing

from loguru import logger

from rialto.jobs.decorators.job_base import JobBase
from rialto.jobs.decorators.resolver import Resolver


def datasource(ds_getter: typing.Callable) -> typing.Callable:
    """
    Dataset reader functions decorator.

    Registers a data-reading function into a rialto job prerequisite.
    You can then request the job via job function arguments.

    :param ds_getter:  dataset reader function
    :return: raw reader function, unchanged
    """
    Resolver.register_callable(ds_getter)
    return ds_getter


def _get_module(stack: typing.List) -> typing.Any:
    last_stack = stack[1]
    mod = inspect.getmodule(last_stack[0])
    return mod


def _get_version(module: typing.Any) -> str:
    try:
        parent_name, _, _ = module.__name__.partition(".")
        return importlib.metadata.version(parent_name)

    except Exception:
        logger.warning(f"Failed to get library {module.__name__} version!")
        return "N/A"


def _generate_rialto_job(callable: typing.Callable, module: object, class_name: str, version: str) -> typing.Type:
    generated_class = type(
        class_name,
        (JobBase,),
        {
            "get_custom_callable": lambda _: callable,
            "get_job_version": lambda _: version,
            "get_job_name": lambda _: class_name,
        },
    )

    generated_class.__module__ = module.__name__
    setattr(module, class_name, generated_class)

    logger.info(f"Registered {class_name} in {module}")
    return generated_class


def job(name_or_callable: typing.Union[str, typing.Callable]) -> typing.Union[typing.Callable, typing.Type]:
    """
    Rialto jobs decorator.

    Transforms a python function into a rialto transormation, which can be imported and ran by Rialto Runner.
    Allows a custom name, via @job("custom_name_here") or can be just used as @job and the function's name is used.

    :param name_or_callable:  str for custom job name. Otherwise, run function.
    :return: One more job wrapper for run function (if custom name specified).
             Otherwise, generates Rialto Transformation Type and returns it for in-module registration.
    """
    stack = inspect.stack()

    module = _get_module(stack)
    version = _get_version(module)

    if type(name_or_callable) is str:

        def inner_wrapper(callable):
            return _generate_rialto_job(callable, module, name_or_callable, version)

        return inner_wrapper

    else:
        name = name_or_callable.__name__
        return _generate_rialto_job(name_or_callable, module, name, version)
