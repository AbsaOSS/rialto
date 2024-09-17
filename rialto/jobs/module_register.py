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

__all__ = ["ModuleRegister", "register_dependency_module", "register_dependency_callable"]

from rialto.common.utils import get_caller_module


class ModuleRegister:
    """
    Module register. Class which is used by @datasource and @config_parser decorators to register callables / getters.
    Resolver, when searching for a getter for f() defined in module M, uses find_callable("f", "M").
    """

    _storage = {}
    _dependency_tree = {}

    @classmethod
    def add_callable_to_module(cls, callable, module_name):
        """
        Adds a callable to the specified module's storage.

        :param callable: The callable to be added.
        :param module_name: The name of the module to which the callable is added.
        """
        module_callables = cls._storage.get(module_name, [])
        module_callables.append(callable)

        cls._storage[module_name] = module_callables

    @classmethod
    def register_callable(cls, callable):
        """
        Registers a callable by adding it to the module's storage.

        :param callable: The callable to be registered.
        """
        callable_module = callable.__module__
        cls.add_callable_to_module(callable, callable_module)

    @classmethod
    def register_dependency(cls, caller_module, module):
        """
        Registers a module as a dependency of the caller module.

        :param caller_module: The module that is registering the dependency.
        :param module: The module to be registered as a dependency.
        """
        module_dep_tree = cls._dependency_tree.get(caller_module, [])
        module_dep_tree.append(module)

        cls._dependency_tree[caller_module] = module_dep_tree

    @classmethod
    def find_callable(cls, callable_name, module_name):
        """
        Finds a callable by its name in the specified module and its dependencies.

        :param callable_name: The name of the callable to find.
        :param module_name: The name of the module to search in.
        :return: The found callable or None if not found.
        """

        # Loop through this module, and its dependencies
        searched_modules = [module_name] + cls._dependency_tree.get(module_name, [])
        for module in searched_modules:
            # Loop through all functions registered in the module
            for func in cls._storage.get(module, []):
                if func.__name__ == callable_name:
                    return func


def register_dependency_module(module):
    """
    Registers a module as a dependency of the caller module.

    :param module: The module to be registered as a dependency.
    """
    caller_module = get_caller_module().__name__
    ModuleRegister.register_dependency(caller_module, module.__name__)


def register_dependency_callable(callable):
    """
    Registers a callable as a dependency of the caller module.
    Note that the function will be added to the module's list of available dependencies.

    :param callable: The callable to be registered as a dependency.
    """
    caller_module_name = get_caller_module().__name__
    ModuleRegister.add_callable_to_module(callable, caller_module_name)