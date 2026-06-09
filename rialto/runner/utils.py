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

__all__ = ["find_dependency"]

from rialto.runner.services.config_loader import PipelineConfig


def find_dependency(config: PipelineConfig, name: str):
    """
    Get dependency from config

    :param config: Pipeline configuration
    :param name: Dependency name
    :return: Dependency object
    """
    for dep in config.dependencies:
        if dep.name == name:
            return dep
    return None
