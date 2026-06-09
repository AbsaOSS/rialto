#  Copyright 2022-2026 ABSA Group Limited
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

from unittest.mock import MagicMock

from rialto.runner.utils import find_dependency


def _config_with_dependencies(dep_names):
    config = MagicMock()
    deps = []
    for n in dep_names:
        d = MagicMock()
        d.name = n
        deps.append(d)
    config.dependencies = deps
    return config


def test_find_dependency_returns_matching_dependency():
    config = _config_with_dependencies(["dep_a", "dep_b", "dep_c"])

    result = find_dependency(config, "dep_b")

    assert result is config.dependencies[1]


def test_find_dependency_returns_none_when_not_found():
    config = _config_with_dependencies(["dep_a", "dep_b"])

    result = find_dependency(config, "missing_dep")

    assert result is None
