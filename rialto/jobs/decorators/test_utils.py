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
from unittest.mock import patch


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
