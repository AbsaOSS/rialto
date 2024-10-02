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


class ModuleRegisterException(Exception):
    """Module Register Exception - Usually, means a clash in the dependencies"""

    pass


class ModuleRegister:
    """
    Module register. Class which is used by @datasource and @config_parser decorators to register callables / getters.

    Resolver, when searching for a getter for f() defined in module M, uses find_callable("f", "M").
    """

    _storage = {}
    _dependency_tree = {}

    @classmethod
    def add_callable_to_module(cls, callable, parent_name):
        """
        Add a callable to the specified module's storage.

        :param callable: The callable to be added.
        :param parent_name: The name of the module to which the callable is added.
        """
        module_callables = cls._storage.get(parent_name, [])
        module_callables.append(callable)

        cls._storage[parent_name] = module_callables

    @classmethod
    def register_callable(cls, callable):
        """
        Register a callable by adding it to the module's storage.

        :param callable: The callable to be registered.
        """
        callable_module = callable.__module__
        cls.add_callable_to_module(callable, callable_module)

    @classmethod
    def remove_module(cls, module):
        """
        Remove a module from the storage.

        :param module: The module to be removed.
        """
        cls._storage.pop(module.__name__, None)

    @classmethod
    def register_dependency(cls, module, parent_name):
        """
        Register a module as a dependency of the caller module.

        :param module: The module to be registered as a dependency.
        :param parent_name: The module that is registering the dependency.

        """
        module_dep_tree = cls._dependency_tree.get(parent_name, [])
        module_dep_tree.append(module)

        cls._dependency_tree[parent_name] = module_dep_tree

    @classmethod
    def find_callable(cls, callable_name, module_name):
        """
        Find a callable by its name in the specified module and its dependencies.

        :param callable_name: The name of the callable to find.
        :param module_name: The name of the module to search in.
        :return: The found callable or None if not found.
        """
        found_functions = []

        # Loop through this module, and its dependencies
        searched_modules = [module_name] + cls._dependency_tree.get(module_name, [])
        for module in searched_modules:
            # Loop through all functions registered in the module
            for func in cls._storage.get(module, []):
                if func.__name__ == callable_name:
                    found_functions.append(func)

        if len(found_functions) == 0:
            return None

        if len(found_functions) > 1:
            raise ModuleRegisterException(f"Multiple functions with the same name {callable_name} found !")

        else:
            return found_functions[0]


def register_dependency_module(module):
    """
    Register a module as a dependency of the caller module.

    :param module: The module to be registered as a dependency.
    """
    caller_module = get_caller_module().__name__
    ModuleRegister.register_dependency(module.__name__, caller_module)


def register_dependency_callable(callable):
    """
    Register a callable as a dependency of the caller module.

    Note that the function will be added to the module's list of available dependencies.

    :param callable: The callable to be registered as a dependency.
    """
    caller_module = get_caller_module().__name__
    ModuleRegister.add_callable_to_module(callable, caller_module)
