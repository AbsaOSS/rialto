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

__all__ = ["disable_job_decorators"]

import importlib
import typing
from contextlib import contextmanager
from unittest.mock import MagicMock, create_autospec, patch

from rialto.jobs.job_base import JobBase
from rialto.jobs.module_register import ModuleRegister
from rialto.jobs.resolver import Resolver, ResolverException


def _passthrough_decorator(*args, **kwargs) -> typing.Callable:
    if len(args) == 0:
        return _passthrough_decorator
    else:
        return args[0]


@contextmanager
def _disable_job_decorators() -> None:
    patches = [
        patch("rialto.jobs.datasource", _passthrough_decorator),
        patch("rialto.jobs.decorators.datasource", _passthrough_decorator),
        patch("rialto.jobs.config_parser", _passthrough_decorator),
        patch("rialto.jobs.decorators.config_parser", _passthrough_decorator),
        patch("rialto.jobs.job", _passthrough_decorator),
        patch("rialto.jobs.decorators.job", _passthrough_decorator),
    ]

    for i in patches:
        i.start()

    yield

    for i in patches:
        i.stop()


@contextmanager
def disable_job_decorators(module) -> None:
    """
    Disables job decorators in a python module. Useful for testing your rialto jobs and datasources.

    :param module: python module with the decorated functions.
    :return: None
    """
    with _disable_job_decorators():
        ModuleRegister.remove_module(module)
        importlib.reload(module)
        yield

    ModuleRegister.remove_module(module)
    importlib.reload(module)


def resolver_resolves(spark, job: JobBase) -> bool:
    """
    Checker method for your dependency resolution.

    If your job's dependencies are all defined and resolvable, returns true.
    Otherwise, throws an exception.

    :param spark: SparkSession object.
    :param job: Job to try and resolve.

    :return: bool, True if job can be resolved
    """
    call_stack = []
    original_resolve_method = Resolver.resolve

    def stack_watching_resolver_resolve(self, callable):
        # Check for cycles
        if callable in call_stack:
            raise ResolverException(f"Circular Dependence in {callable.__name__}!")

        # Append to call stack
        call_stack.append(callable)

        # Create fake method
        fake_method = create_autospec(callable)
        fake_method.__module__ = callable.__module__

        # Resolve fake method
        result = original_resolve_method(self, fake_method)

        # Remove from call stack
        call_stack.remove(callable)

        return result

    with patch("rialto.jobs.job_base.Resolver.resolve", stack_watching_resolver_resolve):
        with patch("rialto.jobs.job_base.JobBase._add_job_version", lambda _, x: x):
            job().run(
                reader=MagicMock(),
                run_date=MagicMock(),
                spark=spark,
                config=MagicMock(),
                metadata_manager=MagicMock(),
                feature_loader=MagicMock(),
            )
        return True
