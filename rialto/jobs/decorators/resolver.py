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

__all__ = ["ResolverException", "Resolver"]

import inspect
import typing
from functools import cache


class ResolverException(Exception):
    """Resolver Errors Class - In Most Cases your dependency tree is not complete."""

    pass


class Resolver:
    """
    Resolver handles dependency management between datasets and jobs.

    We register different callables, which can depend on other callables.
    Calling resolve() we attempt to resolve these dependencies.
    """

    _storage = {}

    @classmethod
    def _get_args_for_call(cls, function: typing.Callable) -> typing.Dict[str, typing.Any]:
        result_dict = {}
        signature = inspect.signature(function)

        for param in signature.parameters.values():
            result_dict[param.name] = cls.resolve(param.name)

        return result_dict

    @classmethod
    def register_callable(cls, callable: typing.Callable, name: str = None) -> str:
        """
        Register callable with a given name for later resolution.

        In case name isn't present, function's __name__ attribute will be used.

        :param callable: callable to register (getter)
        :param name: str, custom name, f.__name__ will be used otherwise
        :return: str, name under which the callable has been registered
        """
        if name is None:
            name = getattr(callable, "__name__", repr(callable))

        cls._storage[name] = callable
        return name

    @classmethod
    @cache
    def resolve(cls, name: str) -> typing.Any:
        """
        Search for a callable registered prior and attempt to call it with correct arguents.

        Arguments are resolved recursively according to requirements; For example, if we have
        a(b, c), b(d), and c(), d() registered, then we recursively call resolve() methods until we resolve
        c, d -> b -> a

        :param name: name of the callable to resolve
        :return: result of the callable
        """
        if name not in cls._storage.keys():
            raise ResolverException(f"{name} declaration not found!")

        getter = cls._storage[name]
        args = cls._get_args_for_call(getter)

        return getter(**args)

    @classmethod
    def register_resolve(cls, callable: typing.Callable) -> typing.Any:
        """
        Register and Resolve a callable.

        Combination of the register() and resolve() methods for a simplified execution.

        :param callable: callable to register and immediately resolve
        :return: result of the callable
        """
        name = cls.register_callable(callable)
        return cls.resolve(name)

    @classmethod
    def cache_clear(cls) -> None:
        """
        Clear resolver cache.

        The resolve mehtod caches its results to avoid duplication of resolutions.
        However, in case we re-register some callables, we need to clear cache
        in order to ensure re-execution of all resolutions.

        :return: None
        """
        cls.resolve.cache_clear()
