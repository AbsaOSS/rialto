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

from rialto.jobs.decorators.job_base import JobBase
from rialto.jobs.decorators.resolver import Resolver, ResolverException


def _passthrough_decorator(*args, **kwargs) -> typing.Callable:
    if len(args) == 0:
        return _passthrough_decorator
    else:
        return args[0]


@contextmanager
def _disable_job_decorators() -> None:
    patches = [
        patch("rialto.jobs.decorators.datasource", _passthrough_decorator),
        patch("rialto.jobs.decorators.decorators.datasource", _passthrough_decorator),
        patch("rialto.jobs.decorators.config", _passthrough_decorator),
        patch("rialto.jobs.decorators.decorators.config", _passthrough_decorator),
        patch("rialto.jobs.decorators.job", _passthrough_decorator),
        patch("rialto.jobs.decorators.decorators.job", _passthrough_decorator),
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
        importlib.reload(module)
        yield

    importlib.reload(module)


def resolver_resolves(spark, job: JobBase) -> bool:
    """
    Checker method for your dependency resoultion.

    If your job's dependencies are all defined and resolvable, returns true.
    Otherwise, throws an exception.

    :param spark: SparkSession object.
    :param job: Job to try and resolve.

    :return: bool, True if job can be resolved
    """

    class SmartStorage:
        def __init__(self):
            self._storage = Resolver._storage.copy()
            self._call_stack = []

        def __setitem__(self, key, value):
            self._storage[key] = value

        def keys(self):
            return self._storage.keys()

        def __getitem__(self, func_name):
            if func_name in self._call_stack:
                raise ResolverException(f"Circular Dependence on {func_name}!")

            self._call_stack.append(func_name)

            real_method = self._storage[func_name]
            fake_method = create_autospec(real_method)
            fake_method.side_effect = lambda *args, **kwargs: self._call_stack.remove(func_name)

            return fake_method

    with patch("rialto.jobs.decorators.resolver.Resolver._storage", SmartStorage()):
        job().run(reader=MagicMock(), run_date=MagicMock(), spark=spark)

    return True
