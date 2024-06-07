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

__all__ = ["MetadataManager"]

from typing import List

from delta.tables import DeltaTable

from rialto.metadata.data_classes.feature_metadata import FeatureMetadata
from rialto.metadata.data_classes.group_metadata import GroupMetadata


class MetadataManager:
    """Metadata storage i/o"""

    def __init__(self, session, schema_path: str = None):
        self.spark = session

        self.groups_path = f"{schema_path}.group_metadata"
        self.features_path = f"{schema_path}.feature_metadata"

        self.groups = None
        self.features = None

    def _load_metadata(self):
        if self.groups is None:
            self.groups = self.spark.read.table(self.groups_path)
        if self.features is None:
            self.features = self.spark.read.table(self.features_path)

    def _fetch_group_by_name(self, group_name: str) -> GroupMetadata:
        group = self.groups.filter(self.groups.group_name == group_name).collect()
        if not len(group):
            raise LookupError(f"Group {group_name} not found!")
        return GroupMetadata.from_spark(group[0])

    def _fetch_feature_by_name(self, feature_name: str, group_name: str) -> FeatureMetadata:
        feature = (
            self.features.filter(self.features.group_name == group_name)
            .filter(self.features.feature_name == feature_name)
            .collect()
        )
        if not len(feature):
            raise LookupError(f"Feature {feature_name} in group {group_name} not found!")
        return FeatureMetadata.from_spark(feature[0])

    def _fetch_features(self, group_name: str) -> List:
        return self.features.filter(self.features.group_name == group_name).collect()

    def _add_group(self, group_md: GroupMetadata) -> None:
        groups = DeltaTable.forName(self.spark, self.groups_path)
        df = self.spark.createDataFrame([group_md.to_tuple()], self.groups.schema)

        groups.alias("groups").merge(
            df.alias("updates"), "groups.group_name = updates.group_name "
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def _add_features(self, feature_md: List[FeatureMetadata], group_name: str) -> None:
        features = DeltaTable.forName(self.spark, self.features_path)
        feature_data = [md.to_tuple(group_name) for md in feature_md]
        df = self.spark.createDataFrame(feature_data, self.features.schema)

        features.alias("features").merge(
            df.alias("updates"),
            "features.feature_name = updates.feature_name and features.group_name = updates.group_name",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def update(
        self,
        group_md: GroupMetadata,
        features_md: List[FeatureMetadata],
    ):
        """
        Save or refresh information about generated features and their groups

        :param group_md: GroupMetadata object
        :param features_md: list of FeatureMetadata objects
        :return:
        """
        self._load_metadata()
        self._add_group(group_md)
        self._add_features(features_md, group_md.name)

    def get_feature(self, group_name: str, feature_name: str) -> FeatureMetadata:
        """
        Get metadata of one feature

        :param group_name: string name of feature group
        :param feature_name: string name of feature
        :return: FeatureMetadata object
        """
        self._load_metadata()
        group = self.get_group(group_name)
        feature = self._fetch_feature_by_name(feature_name, group_name)
        return feature.add_group(group)

    def get_group(self, group_name: str) -> GroupMetadata:
        """
        Get metadata of one feature group

        :param group_name: string name of feature group
        :return: GroupMetadata object
        """
        self._load_metadata()
        group = self._fetch_group_by_name(group_name)
        features = self._fetch_features(group_name)
        group.add_features([f.feature_name for f in features])
        return group
