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

__all__ = ["GroupMetadata"]

from dataclasses import dataclass
from typing import List, Tuple

from pyspark.sql import Row
from typing_extensions import Self

from rialto.metadata.enums import Schedule
from rialto.metadata.utils import class_to_catalog_name


@dataclass
class GroupMetadata:
    """A dataclass to hold all information about a feature group"""

    name: str
    frequency: Schedule
    description: str
    key: List[str]
    owner: str
    fs_name: str = None
    features: List[str] = None

    def __repr__(self) -> str:
        """Serialize object to string"""
        return (
            "GroupMetadata("
            f"name={self.name!r}, frequency={self.frequency!r}, "
            f"feature store name={self.fs_name!r},"
            f"description={self.description!r}, key={self.key!r}, "
            f"features={self.features!r}, owner={self.owner!r}"
            ")"
        )

    def add_features(self, features: List[str]) -> Self:
        """
        Add feature list belonging to the group

        :param group: list of feature names
        :return: self
        """
        if len(features):
            self.features = features
        return self

    def to_tuple(self) -> Tuple:
        """
        Serialize to tuple

        :return: tuple with feature group information
        """
        if not self.fs_name:
            self.fs_name = class_to_catalog_name(self.name)
        return (self.name, self.frequency.value, self.description, self.key, self.fs_name, self.owner)

    @classmethod
    def from_spark(cls, schema: Row) -> Self:
        """
        Create new instance from spark row

        :param record: spark row
        :return: new instance
        """
        return GroupMetadata(
            name=schema.group_name,
            fs_name=schema.group_fs_name,
            frequency=Schedule[schema.group_frequency],
            description=schema.group_description,
            key=schema.group_key,
            owner=schema.group_owner
        )
