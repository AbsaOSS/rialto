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

__all__ = ["PysparkFeatureLoader"]

from collections import namedtuple
from datetime import date
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from rialto.common import TableReader
from rialto.common.utils import cast_decimals_to_floats
from rialto.loader.config_loader import FeatureConfig, GroupConfig, get_feature_config
from rialto.loader.interfaces import FeatureLoaderInterface
from rialto.metadata.metadata_manager import (
    FeatureMetadata,
    GroupMetadata,
    MetadataManager,
)


class PysparkFeatureLoader(FeatureLoaderInterface):
    """Implementation of feature loader for pyspark environment"""

    def __init__(
        self,
        spark: SparkSession,
        feature_schema: str,
        metadata_schema: str,
        date_column: str = "INFORMATION_DATE",
    ):
        """
        Init

        :param spark: spark session
        :param data_loader: data loader
        :param metadata_schema: schema location of metadata tables
        """
        super().__init__()
        self.spark = spark
        self.reader = TableReader(spark)
        self.feature_schema = feature_schema
        self.date_col = date_column
        self.metadata = MetadataManager(spark, metadata_schema)

    KeyMap = namedtuple("KeyMap", ["df", "key"])

    def read_group(self, group: str, information_date: date) -> DataFrame:
        """
        Read a feature group by getting the latest partition by date

        :param group: group name
        :param information_date: partition date
        :return: dataframe
        """
        return self.reader.get_latest(
            f"{self.feature_schema}.{group}",
            date_until=information_date,
            date_column=self.date_col,
            uppercase_columns=True,
        )

    def get_feature(self, group_name: str, feature_name: str, information_date: date) -> DataFrame:
        """
        Get single feature

        :param group_name: feature group name
        :param feature_name: feature name
        :param information_date: selected date
        :return: A dataframe containing feature group key and selected feature
        """
        print("This function is untested, use with caution!")
        key = self.get_group_metadata(group_name).key
        return self.read_group(self.get_group_fs_name(group_name), information_date).select(*key, feature_name)

    def get_feature_metadata(self, group_name: str, feature_name: str) -> FeatureMetadata:
        """
        Get single features metadata

        :param group_name: feature group name
        :param feature_name: feature name
        :return: metadata dictionary
        """
        return self.metadata.get_feature(group_name, feature_name)

    def get_group(self, group_name: str, information_date: date) -> DataFrame:
        """
        Get feature group

        :param group_name: feature group name
        :param information_date: selected date
        :return: A dataframe containing feature group key
        """
        print("This function is untested, use with caution!")
        return self.read_group(self.get_group_fs_name(group_name), information_date)

    def get_group_metadata(self, group_name: str) -> GroupMetadata:
        """
        Get feature groups metadata

        :param group_name: feature group name
        :return: metadata dictionary
        """
        return self.metadata.get_group(group_name)

    def get_group_fs_name(self, group_name: str) -> str:
        """
        Return groups file system name

        If given name matches databricks path, i.e. has two dot separators, do nothing.
        Else assume it's a class name and search for fs name in metadata.
        :param group_name: Group name
        :return: group filesystem name
        """
        if len(group_name.split(sep=".")) == 3:
            return group_name
        return self.metadata.get_group(group_name).fs_name

    def _are_all_keys_in(self, keys: List[str], columns: List[str]) -> bool:
        """
        Check if all presented keys are in presented list of columns

        :param keys: list of keys
        :param columns: list of columns
        :return: True/False
        """
        for key in keys:
            if key not in columns:
                return False
        return True

    def _add_prefix(self, df: DataFrame, prefix: str, key: List[str]) -> DataFrame:
        """
        Prefixes all column names except for key

        :param df: dataframe
        :param prefix: prefix
        :param key: list of keys
        :return: renamed dataframe
        """
        for col in df.columns:
            if col not in key:
                df = df.withColumnRenamed(col, f"{prefix}_{col}")
        return df

    def _get_keymaps(self, config: FeatureConfig, information_date: date) -> List[KeyMap]:
        """
        Read all key mapping tables specified by configuration

        :param config: configuration object
        :param information_date: date
        :return: List of tuples of loaded dataframes and their keys
        """
        key_maps = []
        for mapping in config.maps:
            df = self.read_group(self.get_group_fs_name(mapping), information_date).drop("INFORMATION_DATE")
            key = self.metadata.get_group(mapping).key
            key_maps.append(PysparkFeatureLoader.KeyMap(df, key))
        return key_maps

    def _join_keymaps(self, base: DataFrame, key_maps: List[KeyMap]) -> DataFrame:
        if len(key_maps):
            for mapping in key_maps:
                if self._are_all_keys_in(mapping.key, base.columns):
                    base = base.join(mapping.df, mapping.key, "inner")
                    key_maps.remove(mapping)
                    return self._join_keymaps(base, key_maps)
            raise KeyError(f"None of {[x.key for x in key_maps]} can be joined onto {base.columns}")
        else:
            return base

    def _add_feature_group(self, base: DataFrame, df: DataFrame, group_cfg: GroupConfig) -> DataFrame:
        group_key = self.metadata.get_group(group_cfg.group).key
        df = df.select(group_cfg.features + group_key)
        df = self._add_prefix(df, group_cfg.prefix, group_key)
        return base.join(df, group_key, "left")

    def get_features_from_cfg(self, path: str, information_date: date) -> DataFrame:
        """Get multiple features across many groups, fetches latest features in relation to the provided date

        :param path: configuration location
        :param information_date: date for extraction
        """
        config = get_feature_config(path)
        # 1 select keys from base
        base = self.read_group(self.get_group_fs_name(config.base.group), information_date).select(config.base.keys)
        # 2 join maps onto base (resolve keys)
        if config.maps:
            key_maps = self._get_keymaps(config, information_date)
            base = self._join_keymaps(base, key_maps)

        # 3 read, select and join other tables
        for group_cfg in config.selection:
            df = self.read_group(self.get_group_fs_name(group_cfg.group), information_date)
            base = self._add_feature_group(base, df, group_cfg)

        # 4 fix dtypes for pandas conversion
        base = cast_decimals_to_floats(base)

        return base

    def get_metadata_from_cfg(self, path: str) -> Dict[str, FeatureMetadata]:
        """
        Get multiple features metadata from config

        :param path: configuration path
        :return: dictionary feature_name : FeatureMetadata
        """
        result = {}
        config = get_feature_config(path)

        for group_cfg in config.selection:
            for feature in group_cfg.features:
                feature_metadata = self.get_feature_metadata(group_cfg.group, feature)
                feature_name = f"{group_cfg.prefix}_{feature}"
                result[feature_name] = feature_metadata

        return result
