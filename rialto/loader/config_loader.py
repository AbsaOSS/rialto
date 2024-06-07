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

__all__ = ["get_feature_config"]

from typing import List, Optional

from pydantic import BaseModel

from rialto.common.utils import load_yaml


class GroupConfig(BaseModel):
    group: str
    prefix: str
    features: List[str]


class BaseConfig(BaseModel):
    group: str
    keys: List[str]


class FeatureConfig(BaseModel):
    selection: List[GroupConfig]
    base: BaseConfig
    maps: Optional[List[str]] = None


def get_feature_config(path) -> FeatureConfig:
    """
    Read yaml and parse it

    :param path: config path
    :return: Pydantic feature config object
    """
    return FeatureConfig(**load_yaml(path))
