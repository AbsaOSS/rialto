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

__all__ = ["ModuleRegister"]


class ModuleRegister:
    _storage = {}
    _dependency_tree = {}

    @classmethod
    def register_callable(cls, callable):
        callable_module = callable.__module__

        module_callables = cls._storage.get(callable_module, [])
        module_callables.append(callable)

        cls._storage[callable_module] = module_callables

    @classmethod
    def register_dependency(cls, caller_module, module):
        caller_module_name = caller_module.__name__
        target_module_name = module.__name__

        module_dep_tree = cls._dependency_tree.get(caller_module_name, [])
        module_dep_tree.append(target_module_name)

        cls._dependency_tree[caller_module_name] = module_dep_tree

    @classmethod
    def get_registered_callables(cls, module_name):
        callables = cls._storage.get(module_name, [])

        for included_module in cls._dependency_tree.get(module_name, []):
            included_callables = cls.get_registered_callables(included_module)
            callables.extend(included_callables)

        return callables
