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
__all__ = ["FeatureFunction", "FeatureHolder"]

import typing

from rialto.maker.utils import feature_name
from rialto.metadata import FeatureMetadata, ValueType


class FeatureFunction:
    """
    A container for feature generating object

    contains a callable object, it's name, parameters
    """

    def __init__(self, name: str, callable_object: typing.Callable, value_type: ValueType = ValueType.nominal):
        self.name = name
        self.callable = callable_object
        self.parameters: typing.Dict[str, typing.Any] = {}
        self.dependencies: typing.List[str] = []
        self.type = value_type
        self.description = "basic feature"

    def __str__(self) -> str:
        """
        Serialize to string for logging

        :return: string serialized object
        """
        return (
            f"Name: {self.name}\n\t"
            f"Parameters: {self.parameters}\n\t"
            f"Type: {self.get_type()}\n\t"
            f"Description: {self.description}"
        )

    def metadata(self) -> FeatureMetadata:
        """
        Return functions metadata

        :return: metadata dict
        """
        return FeatureMetadata(name=self.get_feature_name(), value_type=self.type, description=self.description)

    def get_feature_name(self) -> str:
        """
        Get feature name enhanced by parameters

        :return: full feature name
        """
        return feature_name(self.name, self.parameters)

    def get_type(self) -> str:
        """
        Get feature value type

        :return: value type
        """
        return self.type.value


class FeatureHolder(list):
    """
    A container for FeatureFunctions used in the feature building process

    Basically just a plain list with unique type name
    """

    def __init__(self):
        super().__init__()

    def get_metadata(self) -> typing.List[FeatureMetadata]:
        """
        Concats metadata from all functions

        :return: List of metadata
        """
        return [func.metadata() for func in self]
