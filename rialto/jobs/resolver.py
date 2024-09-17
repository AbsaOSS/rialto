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

from rialto.jobs.module_register import ModuleRegister


class ResolverException(Exception):
    """Resolver Errors Class - In Most Cases your dependency tree is not complete."""

    pass


class Resolver:
    """
    Resolver handles dependency management between datasets and jobs.

    We register different callables, which can depend on other callables.
    Calling resolve() we attempt to resolve these dependencies.
    """

    def __init__(self):
        self._storage = {}

    def register_object(self, object: typing.Any, name: str) -> None:
        """
        Register an object with a given name for later resolution.

        :param object: object to register (getter)
        :param name: str, custom name
        :return: None
        """

        self.register_getter(lambda: object, name)

    def register_getter(self, callable: typing.Callable, name: str = None) -> str:
        """
        Register callable with a given name for later resolution.

        In case name isn't present, function's __name__ attribute will be used.

        :param callable: callable to register (getter)
        :param name: str, custom name, f.__name__ will be used otherwise
        :return: str, name under which the callable has been registered
        """
        if name is None:
            name = getattr(callable, "__name__", repr(callable))

        if name in self._storage:
            raise ResolverException(f"Resolver already registered {name}!")

        self._storage[name] = callable
        return name

    def _find_getter(self, name: str, module_name) -> typing.Callable:
        if name in self._storage.keys():
            return self._storage[name]

        callable_from_dependencies = ModuleRegister.find_callable(name, module_name)
        if callable_from_dependencies is None:
            raise ResolverException(f"{name} declaration not found!")

        return callable_from_dependencies

    def resolve(self, callable: typing.Callable) -> typing.Dict[str, typing.Any]:
        """
        Take a callable and resolve its dependencies / arguments. Arguments can be
        a) objects registered via register_object
        b) callables registered via register_getter
        c) ModuleRegister registered callables via ModuleRegister.register_callable (+ dependencies)

        Arguments are resolved recursively according to requirements; For example, if we have
        a(b, c), b(d), and c(), d() registered, then we recursively call resolve() methods until we resolve
        c, d -> b -> a

        :param callable: function to resolve
        :return: result of the callable
        """

        arg_list = {}

        signature = inspect.signature(callable)
        module_name = callable.__module__

        for param in signature.parameters.values():
            param_getter = self._find_getter(param.name, module_name)
            arg_list[param.name] = self.resolve(param_getter)

        return callable(**arg_list)
