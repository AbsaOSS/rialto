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

__all__ = ["FeatureMetadata"]

from dataclasses import dataclass
from typing import Tuple

from pyspark.sql import Row
from typing_extensions import Self

from rialto.metadata.data_classes.group_metadata import GroupMetadata
from rialto.metadata.enums import ValueType


@dataclass
class FeatureMetadata:
    """A dataclass to hold all information about a feature"""

    value_type: ValueType
    name: str
    description: str
    group: GroupMetadata = None

    def __repr__(self) -> str:
        """Serialize object to string"""
        return (
            "FeatureMetadata("
            f"name={self.name!r}, value_type={self.value_type!r}, "
            f"description={self.description!r}, group={self.group!r}, "
            ")"
        )

    def to_tuple(self, group_name: str) -> Tuple:
        """
        Serialize to tuple

        :param group_name: Feature group name
        :return: tuple with feature information
        """
        return (self.name, self.value_type.value, self.description, group_name)

    def add_group(self, group: GroupMetadata) -> Self:
        """
        Add group information to metadata

        :param group: Group name
        :return: self
        """
        self.group = group
        return self

    @classmethod
    def from_spark(cls, record: Row) -> Self:
        """
        Create new instance from spark row

        :param record: spark row
        :return: new instance
        """
        return FeatureMetadata(
            value_type=ValueType[record.feature_type],
            name=record.feature_name,
            description=record.feature_description,
        )
